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

package com.thumbtack.becquerel.controllers

import javax.inject.Inject

import akka.actor.ActorSystem
import com.kenshoo.play.metrics.Metrics
import com.thumbtack.becquerel.filters.RunIDFilter
import com.thumbtack.becquerel.util.{Action, PlayRequestAdapter, PlayResponseAdapter, SystemInfo}
import com.thumbtack.becquerel.{BecquerelServiceEntityCollectionProcessor, BecquerelServiceManager, views}
import org.apache.olingo.commons.api.edmx.EdmxReference
import org.apache.olingo.server.api.OData
import play.api.mvc._
import play.api.{Configuration, Environment, Logger}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent._

/**
  * Main HTTP controller for Becquerel.
  *
  * Handles both the status page and OData requests.
  */
class HomeController @Inject() (
  configuration: Configuration,
  environment: Environment,
  actorSystem: ActorSystem,
  serviceManager: BecquerelServiceManager,
  metrics: Metrics,
  systemInfo: SystemInfo
) extends Controller {

  val logger: Logger = Logger(getClass)

  /**
    * Show system status, service configuration, and request headers.
    */
  def index: Action[AnyContent] = Action { request =>
    val env = mutable.LinkedHashMap.empty[String, scala.collection.Map[String, String]]

    env ++= serviceManager.describe

    env ++= systemInfo.describe

    env("Request headers") = {
      val section = mutable.LinkedHashMap.empty[String, String]
      section ++= request.headers.headers
      section
    }

    Ok(views.html.index(env))
  }

  /**
    * Publish a service as OData.
    */
  def odataService(name: String, path: String): Action[RawBuffer] = {
    Action.async(parse.raw) { request =>
      implicit val ec: ExecutionContextExecutor = actorSystem.dispatcher

      val runID = request.headers.get(RunIDFilter.header)

      serviceManager(name).map { service =>
        val odata = OData.newInstance()
        val edm = odata.createServiceMetadata(service.metadata, Seq.empty[EdmxReference].asJava)
        val handler = odata.createHandler(edm)
        handler.register(new BecquerelServiceEntityCollectionProcessor(service, runID))

        logger.info(s"Registered handler for service: $name")

        implicit val requestHeader: RequestHeader = request
        val reverseRouter = routes.HomeController.odataService(name, path)

        // If present, used to select insecure or secure schemes for reverse routes,
        // because sometimes X-Forwarded-Port and X-Forwarded-Proto are lies.
        val reverseURL = configuration.getBoolean("reverseRouteSecureOverride").map { secure =>
          reverseRouter.absoluteURL(secure)
        } getOrElse {
          reverseRouter.absoluteURL()
        }

        val requestAdapter = new PlayRequestAdapter(request, path, reverseURL)
        val responseAdapter = new PlayResponseAdapter()

        handler.process(requestAdapter, responseAdapter)

        responseAdapter.result
      }
    }
  }
}
