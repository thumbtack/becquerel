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

import com.codahale.metrics.MetricRegistry
import com.kenshoo.play.metrics.Metrics
import org.scalatest.FunSuite
import org.scalatestplus.play.OneServerPerSuite
import play.api.Application
import play.api.inject.bind
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.libs.json.JsObject
import play.api.libs.ws.WSClient

import com.thumbtack.becquerel.{BecquerelServiceManager, EnvGuardedSuite, EnvGuardedTests}
import com.thumbtack.becquerel.demo.EsDemoConfig

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * Make some requests to a local copy of Becquerel running with the DVD Store data.
  */
class IntegrationTest extends FunSuite with EnvGuardedTests with OneServerPerSuite with EnvGuardedSuite {

  override val guardEnvVarNames: Seq[String] = Seq("INTEGRATION_TESTS")
  override val blockEnvVarNames: Seq[String] = Seq.empty

  override lazy val app: Application = {
    GuiceApplicationBuilder()
      .overrides(
        // Use an isolated Metrics instance to prevent `A metric named jvm.attribute.vendor already exists` errors.
        bind[Metrics].to(new Metrics {
          override def defaultRegistry: MetricRegistry = new MetricRegistry()
          override def toJson: String = ""
        })
      )
      .build
  }

  val timeout: FiniteDuration = 10.seconds

  /**
    * Wait for a service to initialize, then try to find the movie AIRPORT POTLUCK.
    */
  def findMovie(serviceName: String, prefix: String): Unit = {
    val serviceManager = app.injector.instanceOf[BecquerelServiceManager]
    val wsClient: WSClient = app.injector.instanceOf[WSClient]
    val response = Await.result(
      serviceManager(serviceName)
        .flatMap(_.asInstanceOf[DataSourceService[Any, Any, Any, Any]].metadataPromise.future)
        .flatMap { _ =>
          wsClient.url(
            s"http://localhost:$port/$serviceName/${prefix}products" +
              "?$format=json&$filter=%27AIRPORT%20POTLUCK%27%20eq%20title"
          ).get()
      },
      timeout
    ).json
    assert(response.isInstanceOf[JsObject])
    assert(((response \ "value").head \ "actor").asOpt[String].contains("STEVE BAILEY"))
  }

  envGuardedTest("ES_TESTS")("find AIRPORT POTLUCK in ES") {
    findMovie("es", s"${EsDemoConfig.indexPrefix}")
  }

  envGuardedTest("PG_TESTS")("find AIRPORT POTLUCK in PG") {
    findMovie("pg", "ds2__public__")
  }
}
