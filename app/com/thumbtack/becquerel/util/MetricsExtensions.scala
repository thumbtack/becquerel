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

import java.util.concurrent.Callable

import com.codahale.metrics.{Meter, Timer}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Convenience methods for making Metrics work with Scala.
  */
object MetricsExtensions {

  implicit class TimerExtensions(timer: Timer) {

    def timed[T](block: => T): T = {
      timer.time(new Callable[T] {
        override def call(): T = block
      })
    }

    def timeFuture[T](block: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
      val ctx = timer.time()
      val future = block
      future.onComplete(_ => ctx.stop())
      future
    }
  }

  implicit class MeterExtensions(meter: Meter) {

    def countFutureErrors[T](block: => Future[T])(implicit ec: ExecutionContext): Future[T] = {
      val future = block
      future.onFailure { case _ => meter.mark() }
      future
    }
  }
}
