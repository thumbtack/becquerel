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

package com.thumbtack.becquerel

import java.net.URI
import javax.inject.{Inject, Singleton}

import akka.actor.ActorSystem
import com.kenshoo.play.metrics.Metrics
import org.apache.olingo.commons.api.data.EntityCollection
import org.apache.olingo.commons.api.edm.EdmEntitySet
import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider
import org.apache.olingo.server.api.uri.queryoption._
import play.api.{Configuration, Logger}

import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{ExecutionContext, Future}

/**
  * A thing that can respond to OData queries somehow.
  */
trait BecquerelService {

  protected def bsConfig: BecquerelServiceConfig
  def name: String = bsConfig.name
  def queryTimeout: Duration = bsConfig.queryTimeout
  protected def metrics: Metrics = bsConfig.metrics
  protected def actorSystem: ActorSystem = bsConfig.actorSystem
  protected def metadataEC: ExecutionContext = bsConfig.metadataEC
  protected def queryEC: ExecutionContext = bsConfig.queryEC

  /**
    * Long service name for `SystemInfo`.
    */
  def displayName: String = s"${getClass.getSimpleName} $name"

  /**
    * Complaints go here.
    */
  protected def logger: Logger

  /**
    * Service configuration details for `SystemInfo`.
    */
  def describe: scala.collection.Map[String, String] = Map.empty

  /**
    * Describe this service to Olingo.
    */
  def metadata: CsdlEdmProvider

  /**
    * @return OData entities matching an OData query.
    */
  def query(
    runID: Option[String],
    entitySet: EdmEntitySet,
    baseURI: URI,
    filter: Option[FilterOption],
    search: Option[SearchOption],
    select: Option[SelectOption],
    orderBy: Option[OrderByOption],
    top: Option[TopOption],
    skip: Option[SkipOption]
  ): Future[EntityCollection]

  /**
    * Will be called once before the service is asked to handle queries.
    */
  def start(): Unit = ()

  /**
    * @return Completes once the service has finished any cleanup it needs to do.
    */
  def shutdown(): Future[Unit] = Future.successful(())
}

/**
  * A thing that can build a Becquerel service of a given type.
  *
  * @note Implementations must register themselves with [[BecquerelServiceFactoryRegistry]].
  */
trait BecquerelServiceFactory {
  /**
    * @param conf Configuration block for this service, including its name.
    * @param serviceManager Only used by meta-services that look up other services by name.
    */
  def apply(conf: Configuration, serviceManager: BecquerelServiceManager): BecquerelService
}

/**
  * Common configuration for all `BecquerelService` subclasses.
  *
  * @param name Short service name. Used to build URLs and for debugging purposes.
  * @param queryTimeout How long to wait for query results from this service.
  * @param metrics Performance instrumentation.
  * @param actorSystem Used to schedule metadata refresh.
  * @param metadataEC Execution context for metadata refresh.
  * @param queryEC Execution context for queries.
  */
case class BecquerelServiceConfig(
  name: String,
  queryTimeout: Duration,
  metrics: Metrics,
  actorSystem: ActorSystem,
  metadataEC: ExecutionContext,
  queryEC: ExecutionContext
)

@Singleton
class BecquerelServiceConfigFactory @Inject() (
  actorSystem: ActorSystem,
  queryEC: ExecutionContext,
  metrics: Metrics
) {
  def apply(conf: Configuration): BecquerelServiceConfig = {
    BecquerelServiceConfig(
      name = conf
        .getString("name")
        .ensuring(_.nonEmpty, "name should have been added to service config by service manager")
        .get,
      queryTimeout = conf
        .getMilliseconds("queryTimeout")
        .map(_.millis)
        .getOrElse(1.minute),
      metrics = metrics,
      actorSystem = actorSystem,
      metadataEC = actorSystem
        .dispatchers
        .lookup("contexts.metadataRefresh"),
      queryEC = queryEC
    )
  }
}
