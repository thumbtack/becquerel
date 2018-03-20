/*
 *    Copyright 2018 Thumbtack
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

package com.thumbtack.becquerel.datasources.elasticsearch.mocks

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Duration, FiniteDuration}

import akka.actor.{Cancellable, Scheduler}

/**
  * Mock Akka scheduler that runs everything immediately, but keeps track of how long it would have waited.
  */
//noinspection NotImplementedCode
class MockScheduler extends Scheduler {
  override def schedule(initialDelay: FiniteDuration, interval: FiniteDuration, runnable: Runnable)(implicit executor: ExecutionContext): Cancellable = ???
  override def maxFrequency: Double = ???

  val delays: mutable.Buffer[Duration] = mutable.Buffer.empty[Duration]

  override def scheduleOnce(delay: FiniteDuration, runnable: Runnable)(implicit executor: ExecutionContext): Cancellable = {
    delays += delay
    runnable.run()
    new Cancellable {
      private var cancelled = false

      override def cancel(): Boolean = {
        cancelled = true
        true
      }

      override def isCancelled: Boolean = cancelled
    }
  }
}
