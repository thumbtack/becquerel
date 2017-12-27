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

package com.thumbtack.becquerel.datasources

import java.net.URI
import java.time.{Clock, Instant}
import java.util.concurrent.atomic.AtomicReference
import javax.inject.Inject

import akka.actor.Cancellable
import com.codahale.metrics.{Meter, MetricRegistry, Timer}

import com.thumbtack.becquerel.{BecquerelService, BecquerelServiceConfig, BecquerelServiceConfigFactory}
import com.thumbtack.becquerel.util.BecquerelException
import com.thumbtack.becquerel.util.MetricsExtensions._
import org.apache.olingo.commons.api.data.EntityCollection
import org.apache.olingo.commons.api.edm.EdmEntitySet
import org.apache.olingo.server.api.uri.queryoption._
import play.api.Configuration
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.Random
import scala.util.control.NonFatal

/**
  * Becquerel service backed by a single data source.
  *
  * Fetches metadata for all accessible tables in the background.
  *
  * @note Not usable until that fetch is complete.
  *
  * @tparam Column Column type.
  * @tparam Row Row type.
  * @tparam Schema Schema type.
  * @tparam Query Query type.
  */
trait DataSourceService[Column, Row, Schema, Query] extends BecquerelService {

  protected def dssConfig: DataSourceServiceConfig

  protected override def bsConfig: BecquerelServiceConfig = dssConfig.bsConfig
  protected def namespace: String = dssConfig.namespace
  protected def defaultContainerName: String = dssConfig.defaultContainerName
  protected def metadataRefreshInterval: FiniteDuration = dssConfig.metadataRefreshInterval
  protected def metadataRefreshDelayMax: FiniteDuration = dssConfig.metadataRefreshDelayMax
  protected def clock: Clock = dssConfig.clock
  protected def random: Random = dssConfig.random

  protected val refreshTimer: Timer = metrics.defaultRegistry.timer(MetricRegistry.name("service", name, "refresh"))
  protected val fetchErrorsMeter: Meter = metrics.defaultRegistry.meter(MetricRegistry.name("service", name, "fetchErrors"))
  protected val errorsMeter: Meter = metrics.defaultRegistry.meter(MetricRegistry.name("service", name, "errors"))
  protected val queryTimer: Timer = metrics.defaultRegistry.timer(MetricRegistry.name("service", name, "queryTime"))
  protected val rowsMeter: Meter = metrics.defaultRegistry.meter(MetricRegistry.name("service", name, "rows"))

  protected val metadataRef = new AtomicReference[DataSourceMetadata[Column, Row]]()
  val metadataPromise: Promise[DataSourceMetadata[Column, Row]] = Promise()

  /**
    * @note Must be initialized after metadata connection.
    *       The service manager should do this outside of tests.
    */
  lazy val refresher: Cancellable = {
    implicit val ec: ExecutionContext = metadataEC

    val metadataRefreshDelay = (metadataRefreshDelayMax * random.nextDouble())
      .asInstanceOf[FiniteDuration]

    actorSystem.scheduler
      .schedule(metadataRefreshDelay, metadataRefreshInterval)(refresh())
  }

  /**
    * Start the refresher once this class is fully initialized and the metadata connection is ready.
    */
  override def start(): Unit = refresher

  /**
    * Cancel the metadata refresh task.
    */
  override def shutdown(): Future[Unit] = {
    implicit val ec: ExecutionContext = metadataEC
    super.shutdown()
      .zip(Future {
        if (!refresher.cancel()) {
          throw new BecquerelException(s"Couldn't cancel metadata refresher for $displayName!")
        }
      })
      .map(_ => ())
  }

  /**
    * Get previously fetched metadata.
    *
    * @throws com.thumbtack.becquerel.util.BecquerelException if the metadata hasn't been fetched yet.
    */
  override def metadata: DataSourceMetadata[Column, Row] = {
    val metadataValue = metadataRef.get()
    if (metadataValue == null) {
      throw new BecquerelException(s"Metadata for $displayName hasn't been fetched yet. Please wait.")
    } else {
      metadataValue
    }
  }

  /**
    * Service configuration details for `SystemInfo`.
    */
  override def describe: scala.collection.Map[String, String] = {
    val section = mutable.LinkedHashMap.empty[String, String]
    section ++= super.describe
    section("Metadata last fetched") = try {
      metadata.timeFetched.toString
    } catch {
      case _: BecquerelException => "never"
    }
    section
  }

  /**
    * Periodically called by `refresher` to update metadata.
    */
  def refresh(): Unit = {
    logger.info(s"Starting metadata refresh for $displayName.")
    try {
      val definitions = refreshTimer.timed {
        Await.result(
          fetchDefinitions(),
          metadataRefreshInterval
        )
      }
      val metadata = parseDefinitions(definitions, clock.instant())
      metadataRef.lazySet(metadata)
      metadataPromise.trySuccess(metadata)
      logger.info(s"Updated metadata for $displayName.")
    } catch {
      case NonFatal(e) => logger.error(s"Failed to update metadata for $displayName", e)
    }
  }

  /**
    * Get defintions for all tables in this datasource.
    */
  protected def fetchDefinitions(): Future[Schema]

  /**
    * Parse previously fetched definitions into the Becquerel common format.
    */
  protected def parseDefinitions(definitions: Schema, timeFetched: Instant): DataSourceMetadata[Column, Row]

  /**
    * Translate an OData query into a provider query.
    */
  protected def compile(
    runID: Option[String],
    tableMapper: TableMapper[Column, Row],
    filter: Option[FilterOption],
    search: Option[SearchOption],
    select: Option[SelectOption],
    orderBy: Option[OrderByOption],
    top: Option[TopOption],
    skip: Option[SkipOption]
  ): (RowMapper[Column, Row], Query)

  /**
    * @return Provider rows matching the query, if there are any. May be empty.
    */
  protected def execute(compiledQuery: Query): Future[TraversableOnce[Row]]

  override def query(
    runID: Option[String],
    entitySet: EdmEntitySet,
    baseURI: URI,
    filter: Option[FilterOption],
    search: Option[SearchOption],
    select: Option[SelectOption],
    orderBy: Option[OrderByOption],
    top: Option[TopOption],
    skip: Option[SkipOption]
  ): Future[EntityCollection] = {
    implicit val ec: ExecutionContext = queryEC

    // TODO: restore debug logging for queries. Consider pretty printing.
    val tableMapper = metadata.getTableMapper(entitySet)
    val (rowMapper, compiledQuery) = compile(
      runID,
      tableMapper,
      filter,
      search,
      select,
      orderBy,
      top,
      skip
    )

    logger.debug(s"compiledQuery:\n$compiledQuery")

    errorsMeter.countFutureErrors {
      queryTimer.timeFuture {
        execute(compiledQuery).map { rows =>
          val entityCollection = new EntityCollection()
          rows
            .map(rowMapper(baseURI))
            .foreach(entityCollection.getEntities.add)
          rowsMeter.mark(entityCollection.getEntities.size())
          entityCollection
        }
      }
    }
  }
}

/**
  * Common configuration for all `DataSourceService` subclasses.
  *
  * @param namespace OData namespace of the service.
  * @param defaultContainerName Name for the container of all entity sets.
  * @param metadataRefreshInterval Refresh metadata this often in the background.
  * @param metadataRefreshDelayMax Wait up to this long before starting metadata refresh.
  * @param clock Only used to track the age of cached metadata.
  * @param random Only used to randomize the initial delay for metadata refresh.
  */
case class DataSourceServiceConfig(
  bsConfig: BecquerelServiceConfig,
  namespace: String,
  defaultContainerName: String,
  metadataRefreshInterval: FiniteDuration,
  metadataRefreshDelayMax: FiniteDuration,
  clock: Clock,
  random: Random
)

class DataSourceServiceConfigFactory @Inject() (
  bsConfigFactory: BecquerelServiceConfigFactory,
  clock: Clock,
  random: Random
) {
  def apply(conf: Configuration): DataSourceServiceConfig = {
    DataSourceServiceConfig(
      bsConfig = bsConfigFactory(conf),
      namespace = conf
        .getString("namespace")
        .getOrElse("Becquerel"),
      defaultContainerName = conf
        .getString("defaultContainerName")
        .getOrElse("Data"),
      metadataRefreshInterval = conf
        .getMilliseconds("metadataRefreshInterval")
        .map(_.millis)
        .getOrElse(10.minutes),
      metadataRefreshDelayMax = conf
        .getMilliseconds("metadataRefreshDelayMax")
        .map(_.millis)
        .getOrElse(0.minutes),
      clock = clock,
      random = random
    )
  }
}
