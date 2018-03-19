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

package com.thumbtack.becquerel.controllers

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.xml.{Elem, XML => ScalaXML}

import com.codahale.metrics.MetricRegistry
import com.google.cloud.bigquery.TableDefinition
import com.kenshoo.play.metrics.Metrics
import org.scalatest.FunSuite
import org.scalatestplus.play.OneAppPerSuite
import play.api.inject.bind
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.libs.json.{JsLookupResult, JsValue}
import play.api.test.FakeRequest
import play.api.test.Helpers._
import play.api.{Application, Configuration}

import com.thumbtack.becquerel.BecquerelServiceManager
import com.thumbtack.becquerel.datasources.{DataSourceMetadata, DataSourceService}
import com.thumbtack.becquerel.datasources.bigquery.mocks.{MockBigQuery, MockBqBuilder, MockQueryResponse, MockQueryResult}
import com.thumbtack.becquerel.datasources.bigquery.{BqBuilder, BqService, SharedData}
import com.thumbtack.becquerel.modules.BecquerelServiceModule

class HomeControllerTest extends FunSuite with OneAppPerSuite {
  /**
    * BQ mock with our test schema that returns the same data for every query.
    */
  val bqBuilder = new MockBqBuilder(
    new MockBigQuery(
      datasets = Map(
        SharedData.projectId -> Seq(
          SharedData.datasetId
        )
      ),
      tables = Map(
        SharedData.datasetId -> Seq(
          SharedData.tableId -> TableDefinition.Type.TABLE
        )
      ),
      definitions = Map(
        SharedData.tableId -> SharedData.tableDefinition
      ),
      data = {
        case _ =>
          MockQueryResponse(
            MockQueryResult(
              SharedData.rows
            )
          )
      }
    )
  )

  /**
    * Play application configured to use the BQ mock.
    */
  override lazy val app: Application = {
    GuiceApplicationBuilder(modules = Seq(new BecquerelServiceModule()))
      // Don't use application.conf, which may exist for integration tests.
      .loadConfig(env => Configuration.load(env, Map("config.file" -> "/dev/null")))
      // Configure the metadata refresh executor and a service with a BQ data source.
      .configure(
        Map(
          "contexts" -> Map(
            "metadataRefresh" -> Map(
              "executor" -> "thread-pool-executor",
              "throughput" -> 1,
              "thread-pool-executor" -> Map(
                "fixed-pool-size" -> 20
              )
            )
          ),
          "services" -> Map(
            "bq" -> Map(
              "type" -> classOf[BqService].getName,
              "projects" -> Seq(SharedData.projectId),
              "omitProjectID" -> false,
              "namespace" -> SharedData.namespace,
              "metadataRefreshDelayMax" -> "0s"
            )
          )
        )
      )
      .overrides(
        // Use the mock BqBuilder instead of creating actual BigQuery connections.
        bind[BqBuilder].to(bqBuilder),
        // Use an isolated Metrics instance to prevent `A metric named jvm.attribute.vendor already exists` errors.
        bind[Metrics].to(new Metrics {
          override def defaultRegistry: MetricRegistry = new MetricRegistry()
          override def toJson: String = ""
        })
      )
      .build
  }

  def waitForMetadataFetch(): Unit = {
    Await.result(
      app
        .injector
        .instanceOf[BecquerelServiceManager]
        .apply("bq")
        .flatMap[DataSourceMetadata[_, _]] { service =>
          service
            .asInstanceOf[DataSourceService[_, _, _, _]]
            .metadataPromise
            .future
        },
      10.seconds
    )
  }

  test("get status page") {
    val Some(response) = route(app, FakeRequest(GET, "/"))
    assert(status(response) === OK)
    assert(contentType(response).contains(HTML))
    assert(contentAsString(response).contains("Becquerel status"))
  }

  test("get service") {
    waitForMetadataFetch()

    val Some(response) = route(app, FakeRequest(GET, "/bq/").withHeaders("Accept" -> JSON))
    assert(status(response) === OK)
    assert(contentType(response).exists(_.startsWith(JSON)))

    val json: JsValue = contentAsJson(response)
    assert(((json \ "value").head \ "name").as[String] === "project__test__customers")
  }

  test("get metadata") {
    waitForMetadataFetch()

    val Some(response) = route(app, FakeRequest(GET, "/bq/$metadata"))
    assert(status(response) === OK)
    assert(contentType(response).contains(XML))

    val xml: Elem = ScalaXML.loadString(contentAsString(response))
    assert((xml \\ "Schema" \@ "Namespace") === SharedData.namespace)
    assert((xml \\ "Schema" \ "EntityType" \@ "Name") === "project__test__customers")
  }

  test("run query") {
    waitForMetadataFetch()

    val Some(response) = route(app, FakeRequest(GET, "/bq/project__test__customers").withHeaders("Accept" -> JSON))
    assert(status(response) === OK)
    assert(contentType(response).exists(_.startsWith(JSON)))

    val json: JsValue = contentAsJson(response)
    val firstRow: JsLookupResult = (json \ "value").head
    assert((firstRow \ "first_name").as[String] === "Jessica")
    assert((firstRow \ "address" \ "country").as[String] === "Arrakis")
  }
}
