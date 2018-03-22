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

package com.thumbtack.becquerel.util

import javax.inject.{Inject, Singleton}

import com.typesafe.config.ConfigException.WrongType
import play.api.inject.Modules
import play.api.{Configuration, Environment}

import com.twitter.util.StorageUnit

import com.thumbtack.becquerel.BuildInfo
import com.thumbtack.becquerel.util.Glob._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.matching.Regex

/**
  * Show system status and configuration.
  */
@Singleton
class SystemInfo @Inject() (
  configuration: Configuration,
  environment: Environment
) {

  /**
    * Get the whole thing as a nested map.
    */
  def describe: scala.collection.Map[String, scala.collection.Map[String, String]] = {
    val env = mutable.LinkedHashMap.empty[String, scala.collection.Map[String, String]]
    env("Build info") = buildInfo
    env("Play") = play
    env("System environment variables") = envVars
    env("Java runtime") = runtime
    env("Java properties") = javaProps
    env
  }

  /**
    * Build server info (not present in development builds).
    */
  def buildInfo: scala.collection.Map[String, String] = {
    val section = mutable.LinkedHashMap.empty[String, String]
    BuildInfo.gitShortRevision.foreach(section("Git revision (short)") = _)
    BuildInfo.gitBranchName.foreach(section("Git branch name") = _)
    BuildInfo.ciBuildTag.foreach(section("CI build tag") = _)
    section
  }

  /**
    * Play configuration.
    */
  def play: scala.collection.Map[String, String] = {
    val section = mutable.LinkedHashMap.empty[String, String]
    section("Environment mode") = environment.mode.toString
    Modules
      .locate(environment, configuration)
      .map(_.getClass.getName)
      .sorted
      .foreach { moduleName => section(moduleName) = "enabled"}
    section
  }

  /**
    * Don't show passwords and API keys in their entirety.
    */
  protected def censor(varType: String, name: String, value: String): String = {
    // Previous versions supported a single glob pattern; now we support lists as well.
    val censorGlobsPath = s"censor.$varType"
    val censorMatcher: Option[Regex] = try {
      configuration
        .getStringSeq(censorGlobsPath)
        .map(Glob.compile)
    } catch {
      case NonFatal(e) if e.getCause.isInstanceOf[WrongType] =>
        configuration
          .getString(censorGlobsPath)
          .map(Glob.compile)
    }

    val shouldCensor = censorMatcher.exists(_.matches(name))
    if (shouldCensor) {
      value.substring(0, SystemInfo.CensorPrefixLength) + "…"
    } else {
      value
    }
  }

  /**
    * System environment variables.
    */
  def envVars: scala.collection.Map[String, String] = {
    val section = mutable.LinkedHashMap.empty[String, String]
    System.getenv.asScala.toSeq.sorted.foreach { case (name, value) =>
      section(name) = censor("env", name, value)
    }
    section
  }

  /**
    * Java properties.
    */
  def javaProps: scala.collection.Map[String, String] = {
    val section = mutable.LinkedHashMap.empty[String, String]
    System.getProperties.asScala.toSeq.sorted.foreach { case (name, value) =>
      section(name) = censor("javaProp", name, value)
    }
    section
  }

  /**
    * CPU and memory resources available to the JVM.
    */
  def runtime: scala.collection.Map[String, String] = {
    val section = mutable.LinkedHashMap.empty[String, String]
    val runtime = Runtime.getRuntime
    section("Available processors") = runtime.availableProcessors().toString
    section("Free memory") = StorageUnit.fromBytes(runtime.freeMemory()).toHuman()
    section("Total memory") = StorageUnit.fromBytes(runtime.totalMemory()).toHuman()
    section("Max memory") = StorageUnit.fromBytes(runtime.maxMemory()).toHuman()
    section
  }
}

object SystemInfo {
  val CensorPrefixLength = 4
}
