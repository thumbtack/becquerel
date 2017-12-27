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

package com.thumbtack.becquerel.util

import java.net.InetAddress
import java.util.concurrent.TimeUnit
import javax.inject.{Inject, Singleton}

import com.codahale.metrics.ScheduledReporter
import com.google.common.collect.{ImmutableMap, ImmutableSet}
import com.izettle.metrics.dw.{InfluxDbReporterFactory, SenderType}
import com.kenshoo.play.metrics.Metrics
import play.api.Configuration
import play.api.inject.ApplicationLifecycle

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Try

trait InfluxDBReporter {}

/**
  * Create an InfluxDB reporter and start it when this class is instantiated, and stop it at app shutdown.
  */
@Singleton
class InfluxDBReporterImpl @Inject() (
  configuration: Configuration,
  lifecycle: ApplicationLifecycle,
  metrics: Metrics
) extends InfluxDBReporter {
  assert(configuration.getConfig("metrics.influxdb").nonEmpty)

  private val reporter: ScheduledReporter = {
    val factory = new InfluxDbReporterFactory()

    configuration.getString("metrics.influxdb.protocol").foreach(factory.setProtocol)
    configuration.getString("metrics.influxdb.host").foreach(factory.setHost)
    configuration.getInt("metrics.influxdb.port").foreach(factory.setPort)
    configuration.getString("metrics.influxdb.database").foreach(factory.setDatabase)
    configuration.getString("metrics.influxdb.prefix").foreach(factory.setPrefix)

    configuration.getString("metrics.influxdb.senderType").foreach {
      senderType => factory.setSenderType(SenderType.valueOf(senderType))
    }

    for (
      username <- configuration.getString("metrics.influxdb.username");
      password <- configuration.getString("metrics.influxdb.password")
    ) {
      factory.setAuth(s"$username:$password")
    }

    val tagsBuilder = Map.newBuilder[String, String]
    configuration.getString("metrics.influxdb.hostnameTag")
      .foreach { key =>
        tagsBuilder += key -> InetAddress.getLocalHost.getHostName
      }
    configuration.getConfig("metrics.influxdb.tags")
      .foreach { tagsConf =>
        tagsConf.subKeys.foreach { key =>
          tagsConf.getString(key).foreach { value =>
            tagsBuilder += key -> value
          }
        }
      }
    factory.setTags(tagsBuilder.result().asJava)

    configuration.getConfig("metrics.influxdb.fields")
      .map { fieldsConf =>
        val fieldsBuilder = ImmutableMap.builder[String, ImmutableSet[String]]
        fieldsConf.subKeys.foreach { key =>
          val setBuilder = ImmutableSet.builder[String]
          fieldsConf.getStringList(key).foreach { values =>
            setBuilder.addAll(values)
          }
          fieldsBuilder.put(key, setBuilder.build())
        }
        fieldsBuilder.build()
      }
      .foreach(factory.setFields)

    factory.build(metrics.defaultRegistry)
  }

  configuration.getMilliseconds("metrics.influxdb.interval") match {
    case Some(interval) => reporter.start(interval, TimeUnit.MILLISECONDS)
    case _ => throw new BecquerelException("metrics.influxdb.interval must be set to use InfluxDB!")
  }

  lifecycle.addStopHook { () => Future.fromTry(Try(reporter.stop())) }
}
