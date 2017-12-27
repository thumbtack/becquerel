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

package com.thumbtack.becquerel.datasources.jdbc

import java.time.Clock

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.util.Random

import akka.actor.ActorSystem
import com.kenshoo.play.metrics.MetricsImpl
import org.scalatest.FunSuite
import play.api.Configuration
import play.api.db.Databases
import play.api.inject.DefaultApplicationLifecycle

import com.thumbtack.becquerel.{BecquerelServiceConfig, EnvGuardedTests}
import com.thumbtack.becquerel.datasources.DataSourceServiceConfig

class JdbcServiceTest extends FunSuite with EnvGuardedTests {

  val actorSystem = ActorSystem()
  implicit val ec: ExecutionContext = actorSystem.dispatcher

  envGuardedTest("PG_TESTS")("fetch table definitions") {
    val jdbcService = new JdbcService(
      JdbcServiceConfig(
        dssConfig = DataSourceServiceConfig(
          bsConfig = BecquerelServiceConfig(
            name = "jdbc",
            queryTimeout = 1.minute,
            metrics = new MetricsImpl(new DefaultApplicationLifecycle(), Configuration.empty),
            actorSystem = ActorSystem(),
            metadataEC = ec,
            queryEC = ec
          ),
          namespace = "JDBC",
          defaultContainerName = "Data",
          metadataRefreshInterval = 5.minutes,
          metadataRefreshDelayMax = Duration.Zero,
          clock = Clock.systemUTC(),
          random = Random
        ),
        db = Databases("org.postgresql.Driver", "jdbc:postgresql://localhost/ds2"),
        omitCatalogID = true,
        omitSchemaID = true,
        safeTableTypes = Set("TABLE", "VIEW")
      )
    )

    jdbcService.refresh()
    assert(jdbcService.metadata.entityTypes.nonEmpty)
  }
}
