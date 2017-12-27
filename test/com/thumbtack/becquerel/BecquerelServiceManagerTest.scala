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

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.Clock

import akka.actor.ActorSystem
import com.codahale.metrics.MetricRegistry
import com.google.cloud.bigquery.TableDefinition
import com.kenshoo.play.metrics.Metrics
import com.thumbtack.becquerel.datasources.DataSourceServiceConfigFactory
import com.thumbtack.becquerel.datasources.bigquery.mocks.{MockBigQuery, MockBqBuilder}
import com.thumbtack.becquerel.datasources.bigquery.{BqService, BqServiceConfigFactory, BqServiceFactory, SharedData}
import org.scalatest.FunSuite
import play.api.Configuration
import play.api.inject.DefaultApplicationLifecycle

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

class BecquerelServiceManagerTest extends FunSuite {
  /**
    * Test credentials file for BigQuery. Don't worry, the key isn't a real one.
    */
  val credentialsPath = "test/com/thumbtack/becquerel/datasources/bigquery/test-creds.json"

  def testServiceWithConfig(serviceConfig: Map[String, Any]): Unit = {
    val serviceName = "bq"

    val configuration = Configuration.from(
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
          serviceName -> (Map(
            "type" -> classOf[BqService].getName,
            "projects" -> Seq(SharedData.projectId),
            "namespace" -> SharedData.namespace
          ) ++ serviceConfig)
        )
      )
    )
    val actorSystem = ActorSystem("default", configuration.underlying)
    val ec = actorSystem.dispatcher
    // Use an isolated Metrics instance to prevent `A metric named jvm.attribute.vendor already exists` errors.
    val metrics = new Metrics {
      override def defaultRegistry: MetricRegistry = new MetricRegistry()
      override def toJson: String = ""
    }
    val factoryRegistry = new BecquerelServiceFactoryRegistry()
    // Register the service factory by creating one.
    new BqServiceFactory(
      factoryRegistry,
      new BqServiceConfigFactory(
        new DataSourceServiceConfigFactory(
          new BecquerelServiceConfigFactory(
            actorSystem,
            ec,
            metrics
          ),
          Clock.systemUTC(),
          Random
        ),
        new MockBqBuilder(
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
            )
          )
        )
      )
    )
    val lifecycle = new DefaultApplicationLifecycle()
    try {
      val bqServiceManager = new BecquerelServiceManagerImpl(
        configuration = configuration,
        ec = ec,
        lifecycle = lifecycle,
        factoryRegistry = factoryRegistry
      )

      // Force metadata refresh from mocked BigQuery.
      val bqService = Await.result(bqServiceManager(serviceName), 10.seconds).asInstanceOf[BqService]
      bqService.refresh()
      assert(bqService.metadata.tableMappers.size === 1)
    } finally {
      // Shut down the background metadata refresh threads.
      Await.result(lifecycle.stop(), 10.seconds)
    }
  }

  test("create BigQuery service from credentials file") {
    testServiceWithConfig(Map(
      "credentials" -> credentialsPath
    ))
  }

  test("create BigQuery service from credentials JSON") {
    testServiceWithConfig(Map(
      "credentialsJSON" -> new String(
        Files.readAllBytes(Paths.get(credentialsPath)),
        StandardCharsets.UTF_8)
    ))
  }
}
