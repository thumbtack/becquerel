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

import java.util.concurrent.TimeUnit

import akka.dispatch._
import com.typesafe.config.Config
import org.slf4j.MDC

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * Configurator for an MDC propagating dispatcher.
  *
  * To use it, configure play like this:
  * {{{
  * play {
  *   akka {
  *     actor {
  *       default-dispatcher = {
  *         type = "com.thumbtack.becquerel.util.MDCPropagatingDispatcherConfigurator"
  *       }
  *     }
  *   }
  * }
  * }}}
  *
  * Credits to James Roper for the [[https://github.com/jroper/thread-local-context-propagation initial implementation]]
  * and Yann Simon for [[https://yanns.github.io/blog/2014/05/04/slf4j-mapped-diagnostic-context-mdc-with-play-framework/ this version]].
  */
class MDCPropagatingDispatcherConfigurator(config: Config, prerequisites: DispatcherPrerequisites)
  extends MessageDispatcherConfigurator(config, prerequisites) {

  private val instance = new MDCPropagatingDispatcher(
    this,
    config.getString("id"),
    config.getInt("throughput"),
    FiniteDuration(config.getDuration("throughput-deadline-time", TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS),
    configureExecutor(),
    FiniteDuration(config.getDuration("shutdown-timeout", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS))

  override def dispatcher(): MessageDispatcher = instance
}

/**
  * An MDC propagating dispatcher.
  *
  * This dispatcher propagates the MDC current request context if it's set when it's executed,
  * and thus allows SLF4J to write logs with information about the current HTTP request.
  */
class MDCPropagatingDispatcher(
  _configurator: MessageDispatcherConfigurator,
  id: String,
  throughput: Int,
  throughputDeadlineTime: Duration,
  executorServiceFactoryProvider: ExecutorServiceFactoryProvider,
  shutdownTimeout: FiniteDuration
) extends Dispatcher(
  _configurator,
  id,
  throughput,
  throughputDeadlineTime,
  executorServiceFactoryProvider,
  shutdownTimeout
) {
  self =>

  override def prepare(): ExecutionContext = new ExecutionContext {
    /**
      * Capture the MDC.
      */
    val mdcContext: java.util.Map[String, String] = MDC.getCopyOfContextMap

    override def execute(r: Runnable): Unit = self.execute(new Runnable {
      override def run(): Unit = {
        // Back up the callee MDC context.
        val oldMDCContext = MDC.getCopyOfContextMap

        // Run the runnable with the captured context.
        setContextMap(mdcContext)
        try {
          r.run()
        } finally {
          // Restore the callee MDC context.
          setContextMap(oldMDCContext)
        }
      }
    })

    override def reportFailure(t: Throwable): Unit = self.reportFailure(t)
  }

  private[this] def setContextMap(context: java.util.Map[String, String]) {
    if (context == null) {
      MDC.clear()
    } else {
      MDC.setContextMap(context)
    }
  }
}
