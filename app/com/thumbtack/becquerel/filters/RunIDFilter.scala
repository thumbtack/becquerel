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

package com.thumbtack.becquerel.filters

import java.util.UUID
import javax.inject.Inject

import akka.stream.Materializer
import org.slf4j.MDC
import play.api.mvc.{Filter, RequestHeader, Result}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Retrieve or generate a run ID string, and add it to the request headers, response headers, and logger context.
  *
  * Used for tracing the actions taken in response to a request.
  */
class RunIDFilter @Inject() (implicit val mat: Materializer, ec: ExecutionContext) extends Filter {
  def apply(nextFilter: RequestHeader => Future[Result])(requestHeader: RequestHeader): Future[Result] = {

    val runID: String = requestHeader.headers.get(RunIDFilter.header)
      .getOrElse(UUID.randomUUID().toString)
    val decoratedRequestHeader = requestHeader.copy(
      headers = requestHeader.headers.replace(RunIDFilter.header -> runID)
    )
    MDC.put(RunIDFilter.mdc, runID)

    val decoratedResult = nextFilter(decoratedRequestHeader).map { result =>
      result.withHeaders(RunIDFilter.header -> runID)
    }

    MDC.remove(RunIDFilter.mdc)
    decoratedResult
  }
}

object RunIDFilter {
  /**
    * Name of the HTTP header containing the run ID.
    */
  val header = "Run-ID"

  /**
    * Name of the MDC entry containing the run ID.
    */
  val mdc = "run_id"
}
