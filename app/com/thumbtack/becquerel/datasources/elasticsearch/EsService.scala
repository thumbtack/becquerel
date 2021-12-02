/*
 *    Copyright 2017–2018 Thumbtack
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.thumbtack.becquerel.datasources.elasticsearch

import java.time.Instant
import javax.inject.{Inject, Singleton}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import play.api.Logger
import play.api.Configuration
import play.api.http.Status

import com.codahale.metrics.{Gauge, MetricRegistry}
import com.sksamuel.elastic4s.Hit
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties}
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.indexes.IndexMappings
import com.sksamuel.elastic4s.requests.searches.SearchRequest
import org.apache.olingo.server.api.uri.queryoption._
import com.thumbtack.becquerel.datasources._
import com.thumbtack.becquerel.util.BecquerelException
import com.thumbtack.becquerel.{BecquerelServiceFactory, BecquerelServiceFactoryRegistry, BecquerelServiceManager}

/**
  * OData service backed by Elasticsearch.
  */
class EsService(
  esConfig: EsServiceConfig
) extends DataSourceService[AnyRef, Hit, Seq[IndexMappings], SearchRequest] {

  protected override def dssConfig: DataSourceServiceConfig = esConfig.dssConfig

  protected override def logger: Logger = Logger(getClass)

  override def displayName: String = s"Elasticsearch service $name"

  override def describe: scala.collection.Map[String, String] = {
    val section = mutable.LinkedHashMap.empty[String, String]
    section ++= super.describe
    section("URL") = esConfig.url
    section("Indexes") = esConfig.indexes.toSeq.sorted.mkString(", ")
    section("Aliases") = esConfig.aliases.toSeq.sorted.mkString(", ")
    section("Retry initial wait") = esConfig.retryInitialWait.toString
    section("Retry max attempts") = esConfig.retryMaxAttempts.toString
    section("Retry status codes") = esConfig.retryStatusCodes.toSeq.sorted.mkString(", ")
    section("Pending retries") = pendingRetries.getValue.toString
    section
  }

  /**
    * Uses [[org.apache.http.nio.client.HttpAsyncClient]], which has its own thread pool.
    */
  protected val actualEsClient = ElasticClient(JavaClient(ElasticProperties(esConfig.url)))

  /**
    * @return A retrying ES client wrapper from this service's actual ES client and retry config.
    */
  protected def newEsClient: ExecutionContext => EsRetryHttpClient = {
    new EsRetryHttpClient(
      actualEsClient,
      actorSystem.scheduler,
      initialWait = esConfig.retryInitialWait,
      maxWait = esConfig.retryMaxWait,
      maxAttempts = esConfig.retryMaxAttempts,
      statusCodes = esConfig.retryStatusCodes
    )(_)
  }

  protected val metadataEsClient: EsRetryHttpClient = newEsClient(metadataEC)
  protected val queryEsClient: EsRetryHttpClient = newEsClient(queryEC)

  protected val pendingRetries: Gauge[Int] = metrics.defaultRegistry.register(
    MetricRegistry.name("service", name, "pendingRetries"),
    new Gauge[Int] {
      override def getValue: Int = {
        metadataEsClient.numPendingTasks + queryEsClient.numPendingTasks
      }
    })

  protected override def fetchDefinitions(): Future[Seq[IndexMappings]] = {
    implicit val ec: ExecutionContext = metadataEC

    // Fetch the mappings for every index that matches the `indexes` glob.
    val indexMappingsTask: Future[Seq[IndexMappings]] = Future.traverse(esConfig.indexes.toSeq) { glob =>
      metadataEsClient.execute(getMapping(glob))
    }.map (_.flatMap(_.result))

    // Fetch the mappings for every alias that matches an `aliases` glob.
    val aliasMappingsTask: Future[Seq[IndexMappings]] = metadataEsClient.execute(getAliases(Nil, esConfig.aliases.toSeq))
      .flatMap { aliasResponse =>
        val aliases = aliasResponse.result.mappings.values.flatten
        // Get the alias for every index that has an alias.
        // Fetch the mappings for each unique alias.
        Future.traverse(aliases.toSet.toSeq) { alias =>
          metadataEsClient.execute(getMapping(alias.name))
            .map { mappings =>
              val sortedMappings = mappings.result.sortBy(_.index)((Ordering[String].reverse))
              assert(sortedMappings.nonEmpty)
              if (sortedMappings.size > 1) {
                // TODO: schema merge if multiple indexes are present, such as for a partitioned table.
                logger.info(s"Alias ${alias.name} maps to multiple indexes. Using schema from the latest one.")
              }
              // Get the mappings for the first index, but replace the index name with the alias.
              // But, we remember the physical index name in the mapping for use later.
              val updated_mappings = sortedMappings.head.mappings + ("real_index" -> sortedMappings.head.index)
              sortedMappings.head.copy(index = alias.name, mappings = updated_mappings)
            }
        }
      }

    // Combine the two lists of mappings.
    for (
      indexMappings <- indexMappingsTask;
      aliasMappings <- aliasMappingsTask
    ) yield {
      indexMappings ++ aliasMappings
    }
  }

  protected override def parseDefinitions(definitions: Seq[IndexMappings], timeFetched: Instant): DataSourceMetadata[AnyRef, Hit] = {
    DataSourceMetadata(
      namespace = namespace,
      defaultContainerName = defaultContainerName,
      tableMappers = definitions
        .map(EsTableMapper(namespace)),
      timeFetched = timeFetched
    )
  }

  protected override def compile(
    runID: Option[String],
    tableMapper: TableMapper[AnyRef, Hit],
    filter: Option[FilterOption],
    search: Option[SearchOption],
    select: Option[SelectOption],
    orderBy: Option[OrderByOption],
    top: Option[TopOption],
    skip: Option[SkipOption]
  ): (RowMapper[AnyRef, Hit], SearchRequest) = {
    // TODO: decorate with run ID (if there's any logging for these queries)
    EsQueryCompiler.compile(
      tableMapper,
      filter,
      search,
      select,
      orderBy,
      top,
      skip
    )
  }

  override def execute(compiledQuery: SearchRequest): Future[TraversableOnce[Hit]] = {
    implicit val ec: ExecutionContext = queryEC

    queryEsClient
      .execute {
        compiledQuery
      }.map(_.result.hits.hits)
  }

  override def shutdown(): Future[Unit] = {
    implicit val ec: ExecutionContext = metadataEC
    super.shutdown()
      .zip(Future {
        metadataEsClient.close()
        queryEsClient.close()
        actualEsClient.close()
      })
      .map(_ => ())
  }
}

@Singleton
class EsServiceFactory @Inject() (
  factoryRegistry: BecquerelServiceFactoryRegistry,
  esConfigFactory: EsServiceConfigFactory
) extends BecquerelServiceFactory {

  factoryRegistry(classOf[EsService].getName) = this
  private def logger: Logger = Logger(getClass)

  override def apply(conf: Configuration, serviceManager: BecquerelServiceManager): EsService = {
    try {
      val service = new EsService(esConfigFactory(conf))
      logger.info(s"Created EsService $service")
      service
    } catch {
      case e: Exception => logger.error(s"Caught exception: $e")
        throw e
    }
  }
}

/**
  * @param url elasticsearch:// URL for your cluster.
  * @param indexes Multi-index glob for specific indexes to publish to OData.
  *                Pass "*" or "_all" if you want all of them.
  *                See https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-index.html for syntax.
  * @param aliases Same, but for aliases (the index listing API won't pick them up).
  */
case class EsServiceConfig(
  dssConfig: DataSourceServiceConfig,
  url: String,
  indexes: Set[String],
  aliases: Set[String],
  retryInitialWait: FiniteDuration,
  retryMaxWait: FiniteDuration,
  retryMaxAttempts: Int,
  retryStatusCodes: Set[Int]
)

@Singleton
class EsServiceConfigFactory @Inject() (
  dssConfigFactory: DataSourceServiceConfigFactory
) {
  def apply(
    conf: Configuration
  ): EsServiceConfig = {
    EsServiceConfig(
      dssConfig = dssConfigFactory(conf),
      url = conf
        .getString("url")
        .getOrElse {
          val name = conf.getString("name").get
          throw new BecquerelException(s"Elasticsearch service $name must have a url.")
        },
      indexes = conf
        .getStringSeq("indexes")
        .map(_.toSet)
        .getOrElse(Set.empty),
      aliases = conf
        .getStringSeq("aliases")
        .map(_.toSet)
        .getOrElse(Set.empty),
      retryInitialWait = conf
        .getMilliseconds("retry.initialWait")
        .map(_.millis)
        .getOrElse(1.second),
      retryMaxWait = conf
        .getMilliseconds("retry.maxWait")
        .map(_.millis)
        .getOrElse(8.seconds),
      retryMaxAttempts = conf
        .getInt("retry.maxAttempts")
        .getOrElse(5), // scalastyle:ignore magic.number
      retryStatusCodes = conf
        .getIntSeq("retry.statusCodes")
        .map(_.map(_.toInt).toSet)
        .getOrElse(Set(
          Status.TOO_MANY_REQUESTS,
          Status.BAD_GATEWAY,
          Status.SERVICE_UNAVAILABLE,
          Status.GATEWAY_TIMEOUT
        ))
    )
  }
}
