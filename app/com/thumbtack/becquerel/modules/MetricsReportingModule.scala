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

import com.kenshoo.play.metrics._
import com.thumbtack.becquerel.util.{InfluxDBReporter, InfluxDBReporterImpl, PlayMetricsFilter}
import play.api.inject._
import play.api.{Configuration, Environment}

/**
  * Currently responsible for starting an InfluxDB metrics reporter, if configured.
  */
class MetricsReportingModule extends Module {
  override def bindings(environment: Environment, configuration: Configuration): Seq[Binding[_]] = {

    val metricsBindings = Seq.newBuilder[Binding[_]]

    if (configuration.getBoolean("metrics.enabled").getOrElse(true)) {
      metricsBindings += bind[MetricsFilter].to[PlayMetricsFilter].eagerly
      metricsBindings += bind[Metrics].to[MetricsImpl].eagerly

      if (configuration.getConfig("metrics.influxdb").nonEmpty) {
        metricsBindings += bind[InfluxDBReporter].to[InfluxDBReporterImpl].eagerly()
      }
    } else {
      metricsBindings += bind[MetricsFilter].to[DisabledMetricsFilter].eagerly
      metricsBindings += bind[Metrics].to[DisabledMetrics].eagerly
    }

    metricsBindings.result()
  }
}
