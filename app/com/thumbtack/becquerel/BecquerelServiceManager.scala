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

import javax.inject.{Inject, Singleton}

import com.thumbtack.becquerel.util.BecquerelException
import play.api.Configuration
import play.api.inject.ApplicationLifecycle

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait BecquerelServiceManager {

  /**
    * Get a service by name. May not be ready yet.
    */
  def apply(name: String): Future[BecquerelService]

  /**
    * Map of service names to configuration details for `SystemInfo`.
    */
  def describe: scala.collection.Map[String, scala.collection.Map[String, String]] = Map.empty
}

/**
  * Create Becquerel services from the config file.
  */
@Singleton
class BecquerelServiceManagerImpl @Inject() (
  configuration: Configuration,
  implicit val ec: ExecutionContext,
  lifecycle: ApplicationLifecycle,
  factoryRegistry: BecquerelServiceFactoryRegistry
) extends BecquerelServiceManager {

  /**
    * Map of service names to futures for each service.
    */
  private val orderedServices = mutable.LinkedHashMap.empty[String, Future[BecquerelService]]

  /**
    * Create services from the configuration.
    */
  protected def init(): Unit = {

    val confs = configuration
      .getConfig("services")
      .getOrElse {
        throw new BecquerelException("services configuration section is missing!")
      }

    confs
      .subKeys
      .foreach { name =>
        val conf = confs.getConfig(name).get ++ Configuration("name" -> name)
        val typeName = conf.getString("type").getOrElse {
          throw new BecquerelException(s"Service $name must have a type.")
        }
        val future: Future[BecquerelService] = factoryRegistry(typeName).map(_(conf, this))
        orderedServices(name) = future
        future.onSuccess { case service =>
          // Start metadata refresh thread.
          service.start()
          // Stop it when the app stops.
          lifecycle.addStopHook(() => service.shutdown())
        }
        // TODO: add an init timeout and logging if it takes too long.
      }
  }

  override def apply(name: String): Future[BecquerelService] = orderedServices(name)

  override def describe: collection.Map[String, collection.Map[String, String]] = {
    val env = mutable.LinkedHashMap.empty[String, scala.collection.Map[String, String]]
    orderedServices.foreach { case (name, future) =>
      future.value match {
        case Some(Success(service)) => env(service.displayName) = service.describe
        case Some(Failure(t)) => env(s"Failed service $name") = Map("Exception message" -> t.getMessage)
        case None => env(s"Pending service $name") = Map("Status" -> "Still initializing…")
      }
    }
    env
  }

  init()
}
