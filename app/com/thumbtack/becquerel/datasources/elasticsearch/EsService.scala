/*
 *    Copyright 2017 Thumbtack
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
import scala.concurrent.{ExecutionContext, Future}

import com.sksamuel.elastic4s.Hit
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.http.index.mappings.IndexMappings
import com.sksamuel.elastic4s.searches.SearchDefinition
import org.apache.olingo.server.api.uri.queryoption._
import play.api.{Configuration, Logger}

import com.thumbtack.becquerel.datasources._
import com.thumbtack.becquerel.util.BecquerelException
import com.thumbtack.becquerel.{BecquerelServiceFactory, BecquerelServiceFactoryRegistry, BecquerelServiceManager}

/**
  * OData service backed by Elasticsearch.
  */
class EsService(
  esConfig: EsServiceConfig
) extends DataSourceService[AnyRef, Hit, Seq[IndexMappings], SearchDefinition] {

  protected override def dssConfig: DataSourceServiceConfig = esConfig.dssConfig
  protected def url: String = esConfig.url
  protected def indexes: Set[String] = esConfig.indexes
  protected def aliases: Set[String] = esConfig.aliases

  protected override def logger: Logger = Logger(getClass)

  override def displayName: String = s"Elasticsearch service $name"

  override def describe: scala.collection.Map[String, String] = {
    val section = mutable.LinkedHashMap.empty[String, String]
    section ++= super.describe
    section("URL") = url
    section
  }

  /**
    * Uses [[org.apache.http.nio.client.HttpAsyncClient]], which has its own thread pool.
    */
  private val httpClient: HttpClient = HttpClient(url)

  protected override def fetchDefinitions(): Future[Seq[IndexMappings]] = {
    implicit val ec: ExecutionContext = metadataEC

    // Fetch the mappings for every index that matches the `indexes` glob.
    val indexMappingsTask: Future[Seq[IndexMappings]] = Future.traverse(indexes.toSeq) { glob =>
      httpClient.execute {
        // Note: indexes that have disabled mappings with no properties will cause
        // a `java.util.NoSuchElementException` in `GetMappingHttpExecutable`.
        // See https://www.elastic.co/guide/en/elasticsearch/reference/current/enabled.html
        // This looks like an elastic4s bug in parsing the response.
        getMapping(glob)
      }
    }.map(_.flatten)

    // Fetch the mappings for every alias that matches an `aliases` glob.
    val aliasMappingsTask: Future[Seq[IndexMappings]] = Future.traverse(aliases.toSeq) { glob =>
      httpClient.execute {
        getAlias(glob)
      }.flatMap { aliasResponse =>
        // Get the alias for every index that has an alias.
        val aliases = aliasResponse.flatMap { case (_, aliasesForIndex) =>
          aliasesForIndex.get("aliases").toSeq.flatMap(_.keys)
        }
        // Fetch the mappings for each unique alias.
        Future.traverse(aliases.toSet.toSeq) { alias =>
          httpClient.execute {
            getMapping(alias)
          }.map { indexMappings =>
            assert(indexMappings.nonEmpty)
            if (indexMappings.size > 1) {
              // TODO: schema merge if multiple indexes are present, such as for a partitioned table.
              logger.warn(s"Alias $alias maps to multiple indexes. Using schema from the first one.")
            }
            // Get the mappings for the first index, but replace the index name with the alias.
            indexMappings.head.copy(index = alias)
          }
        }
      }
    }.map(_.flatten)

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
  ): (RowMapper[AnyRef, Hit], SearchDefinition) = {
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

  override def execute(compiledQuery: SearchDefinition): Future[TraversableOnce[Hit]] = {
    implicit val ec: ExecutionContext = queryEC
    httpClient.execute {
      compiledQuery
    }.map(_.hits.hits)
  }

  override def shutdown(): Future[Unit] = {
    implicit val ec: ExecutionContext = metadataEC
    super.shutdown()
      .zip(Future { httpClient.close() })
      .map(_ => ())
  }
}

@Singleton
class EsServiceFactory @Inject() (
  factoryRegistry: BecquerelServiceFactoryRegistry,
  esConfigFactory: EsServiceConfigFactory
) extends BecquerelServiceFactory {

  factoryRegistry(classOf[EsService].getName) = this

  override def apply(conf: Configuration, serviceManager: BecquerelServiceManager): EsService = {
    new EsService(esConfigFactory(conf))
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
  aliases: Set[String]
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
        .getOrElse(Set.empty)
    )
  }
}
