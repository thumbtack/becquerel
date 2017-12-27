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

package com.thumbtack.becquerel.modules

import java.sql.Connection
import java.time.Clock
import javax.sql.DataSource

import com.thumbtack.becquerel._
import com.thumbtack.becquerel.datasources.DataSourceServiceConfigFactory
import com.thumbtack.becquerel.datasources.bigquery.{BqBuilder, BqServiceConfigFactory, BqServiceFactory, GoogleBqBuilder}
import com.thumbtack.becquerel.datasources.elasticsearch.{EsServiceConfigFactory, EsServiceFactory}
import com.thumbtack.becquerel.datasources.jdbc.{JdbcServiceConfigFactory, JdbcServiceFactory}
import com.thumbtack.becquerel.util.SystemInfo
import play.api.db.Database
import play.api.inject._
import play.api.{Configuration, Environment}

import scala.util.Random

/**
  * Bindings needed to create and access Becquerel services.
  */
class BecquerelServiceModule extends Module {
  override def bindings(environment: Environment, configuration: Configuration): Seq[Binding[_]] = {
    Seq(
      // Common config parsers.
      bind[BecquerelServiceFactoryRegistry].toSelf,
      bind[BecquerelServiceConfigFactory].toSelf,
      bind[Clock].toInstance(Clock.systemUTC()),
      bind[Random].toInstance(Random),
      bind[DataSourceServiceConfigFactory].toSelf,

      // Eager bindings are necessary to register the factory for each type.
      bind[ShadowServiceConfigFactory].toSelf,
      bind[ShadowServiceFactory].toSelf.eagerly(),

      bind[SystemInfo].toSelf,

      // TODO: refactor data source backends into their own subprojects.
      // This will mean we don't need to link against every possible backend's support libraries.

      bind[BqBuilder].to[GoogleBqBuilder],
      bind[BqServiceConfigFactory].toSelf,
      bind[BqServiceFactory].toSelf.eagerly(),

      bind[EsServiceConfigFactory].toSelf,
      bind[EsServiceFactory].toSelf.eagerly(),

      bind[JdbcServiceConfigFactory].toSelf,
      bind[JdbcServiceFactory].toSelf.eagerly(),

      // Start metadata refresh threads when app starts.
      bind[BecquerelServiceManager].to[BecquerelServiceManagerImpl].eagerly()
    )
  }
}
