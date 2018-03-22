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

// scalastyle:off custom.no-println

package com.thumbtack.becquerel.demo

import scala.collection.JavaConverters._
import scala.concurrent.duration.{Duration, FiniteDuration}

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Config values shared by multiple demo data loaders.
  */
object SharedConfig {
  private[demo] lazy val demo: Config = ConfigFactory.parseResourcesAnySyntax("demo").getConfig("demo")
    .withFallback(ConfigFactory.parseMap(Map(
      "timeout" -> "5m"
    ).asJava))
  lazy val dvdStoreDir: String = demo.getString("dvdStoreDir")
  lazy val timeout: FiniteDuration = Duration.fromNanos(demo.getDuration("timeout").toNanos)

  def main(args: Array[String]): Unit = {
    println(demo.toString)
  }
}
