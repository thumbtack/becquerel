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

package com.thumbtack.becquerel.datasources.elasticsearch

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

import akka.actor.{Cancellable, Scheduler}
import com.sksamuel.elastic4s.http.{HttpClient, HttpExecutable}
import org.elasticsearch.client.{ResponseException, RestClient}

/**
  * Wrap the default elastic4s HTTP client to add retry behavior on rate limiting responses.
  *
  * @param wrapped Instance of the actual elastic4s client.
  * @param scheduler For scheduling retries.
  * @param initialWait Wait this before the first retry. Double it with every subsequent retry.
  * @param maxAttempts Try this many times, counting the initial request.
  * @param statusCodes Retry on these codes. Note that 502, 503, and 504 have special handling
  *                    within [[RestClient]]
  * @param ec Execution context for recovery handlers.
  */
class EsRetryHttpClient(
  wrapped: HttpClient,
  scheduler: Scheduler,
  initialWait: FiniteDuration,
  maxWait: FiniteDuration,
  maxAttempts: Int,
  statusCodes: Set[Int]
)(
  implicit ec: ExecutionContext
) extends HttpClient {

  override def rest: RestClient = wrapped.rest

  /**
    * Keep track of pending retries so we can cancel them on graceful exit with [[close]]
    * (`Scheduler.close` is required to execute all of them if it implements `close` at all).
    */
  protected val pendingTasks: mutable.Set[Cancellable] = mutable.Set.empty

  def numPendingTasks: Int = pendingTasks.size // Intentionally not synchronized since it's just a metric.

  override def close(): Unit = {
    pendingTasks.synchronized {
      pendingTasks.foreach(_.cancel())
    }
    wrapped.close()
  }

  override def execute[T, U](request: T)(implicit exec: HttpExecutable[T, U]): Future[U] = {
    safeExecute(request)
      .recoverWith(retry(request, 1))
  }

  /**
    * Some ES commands actually don't use [[HttpExecutable.RichRestClient.async]],
    * instead running the command synchronously,
    * and thus may throw exceptions instead of returning them inside a [[Future]].
    */
  protected def safeExecute[T, U](request: T)(implicit exec: HttpExecutable[T, U]): Future[U] = {
    try {
      wrapped.execute(request)
    } catch {
      case NonFatal(e) => Future.failed(e)
    }
  }

  /**
    * If the request is retryable, use the Akka scheduler to try again later.
    * Backoff increases exponentially with every attempt.
    */
  protected def retry[T, U](
    request: T,
    attempts: Int
  )(
    implicit exec: HttpExecutable[T, U]
  ): PartialFunction[Throwable, Future[U]] = {

    case e: ResponseException
      if statusCodes.contains(e.getResponse.getStatusLine.getStatusCode)
        && attempts < maxAttempts =>

      val wait: FiniteDuration = (initialWait * Math.pow(2, attempts - 1).toLong).min(maxWait)

      logger.warn(s"Retrying rate-limited Elasticsearch request in $wait (attempt $attempts of $maxAttempts)")

      val promise = Promise[U]()
      val task: Cancellable = scheduler.scheduleOnce(wait) {
        promise.completeWith(
          safeExecute(request)
            .recoverWith(retry(request, attempts + 1))
        )
      }
      pendingTasks.synchronized {
        pendingTasks += task
      }
      promise.future.onComplete(_ => pendingTasks.synchronized {
        pendingTasks -= task
      })
      promise.future
  }
}
