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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.control.NonFatal
import scala.util.matching.Regex
import scala.util.{Failure, Success}

import org.apache.olingo.commons.api.data.{Entity, EntityCollection, Property}
import org.apache.olingo.commons.api.edm.EdmEntitySet
import org.apache.olingo.commons.api.edm.provider.CsdlEdmProvider
import org.apache.olingo.server.api.uri.queryoption._
import play.api.libs.concurrent.Timeout
import play.api.{Configuration, Logger}

import com.thumbtack.becquerel.util.Glob

/**
  * Testing metaservice that wraps two other services.
  *
  * Primary provides the metadata.
  *
  * Shadowed tables: queries both simultaneously, but only returns results from the primary.
  * Fallback tables: queries secondary first, primary if there's a failure or timeout.
  * Other tables: queries primary only.
  */
class ShadowService(
  shadowConfig: ShadowServiceConfig
) extends BecquerelService {

  protected override def logger: Logger = Logger(getClass)

  protected override def bsConfig: BecquerelServiceConfig = shadowConfig.bsConfig
  protected def serviceManager: BecquerelServiceManager = shadowConfig.serviceManager
  protected def primaryName: String = shadowConfig.primaryName
  protected def secondaryName: String = shadowConfig.secondaryName
  protected def secondaryTimeout: Duration = shadowConfig.secondaryTimeout
  protected def shadowTables: Set[String] = shadowConfig.shadowTables
  protected def fallbackTables: Set[String] = shadowConfig.fallbackTables

  protected val shadowRegex: Regex = Glob.compile(shadowTables)
  protected val fallbackRegex: Regex = Glob.compile(fallbackTables)

  protected val primary: Future[BecquerelService] = serviceManager(primaryName)
  protected val secondary: Future[BecquerelService] = serviceManager(secondaryName)

  override def displayName: String = s"Shadow service $name"

  override def describe: scala.collection.Map[String, String] = {
    val section = mutable.LinkedHashMap.empty[String, String]
    section ++= super.describe
    section("Primary name") = primaryName
    section("Secondary name") = secondaryName
    section("Secondary timeout") = secondaryTimeout.toString
    section("Shadow tables") = shadowTables.mkString(", ")
    section("Fallback tables") = fallbackTables.mkString(", ")
    section
  }

  override def metadata: CsdlEdmProvider = primary.value.get.get.metadata // TODO: this could be better

  /**
    * Compare important collection attributes.
    */
  protected def cmpCollections(c1: EntityCollection, c2: EntityCollection): Boolean = {
    val es1 = c1.getEntities.asScala
    val es2 = c2.getEntities.asScala
    if (es1.size != es2.size) {
      logger.warn("collection size mismatch")
      false
    } else {
      es1.zip(es2).forall((cmpEntities _).tupled)
    }
  }

  /**
    * Compare important entity attributes.
    */
  protected def cmpEntities(e1: Entity, e2: Entity): Boolean = {
    // TODO: Ignores metadata columns used by upstream ETL. Make this configurable.
    val ps1 = e1.getProperties.asScala.filter(!_.getName.startsWith("becquerel_")).sortBy(_.getName)
    val ps2 = e2.getProperties.asScala.filter(!_.getName.startsWith("becquerel_")).sortBy(_.getName)
    if (e1.getId != e2.getId) {
      logger.warn(s"ID mismatch\n${e1.getId}\n${e2.getId}")
      false
    } else if (ps1.size != ps2.size) {
      logger.warn("entity size mismatch")
      val ps1Names = ps1.map(_.getName).toSet
      val ps2Names = ps2.map(_.getName).toSet
      logger.warn(s"primary but not secondary: ${(ps1Names -- ps2Names).toSeq.sorted.mkString(", ")}")
      logger.warn(s"secondary but not primary: ${(ps2Names -- ps1Names).toSeq.sorted.mkString(", ")}")
      false
    } else {
      ps1.zip(ps2).forall((cmpProperties _).tupled)
    }
  }

  /**
    * Compare important property attributes.
    */
  protected def cmpProperties(p1: Property, p2: Property): Boolean = {
    if (p1.getName != p2.getName) {
      logger.warn("name mismatch")
      false
    } else if (p1.getValue != p2.getValue) {
      logger.warn(s"value mismatch: ${p1.getName}")
      false
    } else if (p1.getType != p2.getType) {
      logger.warn(s"type mismatch: ${p1.getName}")
      false
    } else if (p1.getValueType != p2.getValueType) {
      logger.warn(s"valueType mismatch: ${p1.getName}")
      false
    } else {
      true
    }
  }

  /**
    * @note Expects the default Play [[ExecutionContext]] to be in scope so that we get the correct logging context.
    */
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

    /**
      * Convenience closure that queries a service, allowing for compilation errors and timeouts.
      */
    def queryAgainst(
      service: Future[BecquerelService],
      timeout: Duration = Duration.Inf
    ): Future[EntityCollection] = {
      // Handle the service failing to compile a query.
      val serviceQuery = try {
        service.flatMap(_.query(runID, entitySet, baseURI, filter, search, select, orderBy, top, skip))
      } catch {
        case NonFatal(e) => Future.failed(e)
      }
      // Guard the secondary query with a timeout future.
      timeout match {
        case finite: FiniteDuration => Timeout.timeout(actorSystem, finite)(serviceQuery)
        case _ => serviceQuery // No timeout.
      }
    }

    entitySet.getName match {

      case shadowRegex(_*) =>
        val primaryQuery = queryAgainst(primary)
        compareShadowResults(
          entitySet,
          primaryQuery,
          queryAgainst(secondary, secondaryTimeout)
        )
        primaryQuery

      case fallbackRegex(_*) =>
        queryAgainst(secondary, secondaryTimeout).recoverWith { case NonFatal(t) =>
          logger.warn("Secondary failed! Falling back to primary.", t)
          queryAgainst(primary)
        }

      case _ =>
        queryAgainst(primary)
    }
  }

  /**
    * Check the secondary query's results against the primary query's results asynchronously.
    * Log the result.
    */
  protected def compareShadowResults(
    entitySet: EdmEntitySet,
    primaryQuery: Future[EntityCollection],
    secondaryQuery: Future[EntityCollection]
  )(implicit ec: ExecutionContext): Unit = {
    for (
      primaryResult <- primaryQuery
    ) {
      secondaryQuery.onComplete {

        case Success(secondaryResult) =>
          val shouldFixKey = try {
            entitySet.getEntityType.getKeyPropertyRefs.asScala
              .exists(_.getProperty.getType.getFullQualifiedName.toString != "Edm.String")
          } catch {
            case NonFatal(_) => false
          }

          // Drop ES's `_id` property and correct the type of the key (as if anything used it).
          // TODO: assumes secondary is ES, although both should be no-ops if it isn't.
          for (entity <- secondaryResult.getEntities.asScala) {
            if (shouldFixKey) {
              entity.setId(new URI(entity.getId.toString.replace("'", "")))
            }
            entity.getProperties.remove(entity.getProperty("_id"))
          }

          val resultsMatch = cmpCollections(primaryResult, secondaryResult)
          logger.info(s"primary ${if (resultsMatch) "=" else "≠"} secondary")

        case Failure(t) => logger.warn("Shadowed secondary failed!", t)
      }
    }
  }
}

@Singleton
class ShadowServiceFactory @Inject() (
  factoryRegistry: BecquerelServiceFactoryRegistry,
  shadowConfigFactory: ShadowServiceConfigFactory
) extends BecquerelServiceFactory {

  factoryRegistry(classOf[ShadowService].getName) = this

  override def apply(
    conf: Configuration,
    serviceManager: BecquerelServiceManager
  ): ShadowService = {
    new ShadowService(shadowConfigFactory(conf, serviceManager))
  }
}

case class ShadowServiceConfig(
  bsConfig: BecquerelServiceConfig,
  serviceManager: BecquerelServiceManager,
  primaryName: String,
  secondaryName: String,
  secondaryTimeout: Duration,
  shadowTables: Set[String],
  fallbackTables: Set[String]
)

@Singleton
class ShadowServiceConfigFactory @Inject() (
  bsConfigFactory: BecquerelServiceConfigFactory
) {
  def apply(
    conf: Configuration,
    serviceManager: BecquerelServiceManager
  ): ShadowServiceConfig = {
    ShadowServiceConfig(
      bsConfig = bsConfigFactory(conf),
      serviceManager = serviceManager,
      primaryName = conf
        .getString("primary")
        .get,
      secondaryName = conf
        .getString("secondary")
        .get,
      secondaryTimeout = conf
        .getMilliseconds("secondaryTimeout")
        .map(_.millis)
        .getOrElse(Duration.Inf),
      shadowTables = conf
        .getStringSeq("shadowTables")
        .map(_.toSet)
        .getOrElse(Set.empty),
      fallbackTables = conf
        .getStringSeq("fallbackTables")
        .map(_.toSet)
        .getOrElse(Set.empty)
    )
  }
}
