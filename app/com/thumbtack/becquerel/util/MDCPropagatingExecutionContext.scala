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

import org.slf4j.MDC
import play.api.libs.concurrent.Execution
import play.api.mvc.{ActionBuilder, Request, Result}

import scala.concurrent.{ExecutionContext, Future}

/**
  * Because [[MDCPropagatingDispatcher]] doesn't work (dev mode only?).
  */
class MDCPropagatingExecutionContext(
  mdcContext: java.util.Map[String, String],
  delegate: ExecutionContext
) extends ExecutionContext {

  override def execute(r: Runnable): Unit = delegate.execute(new Runnable {
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

  private[this] def setContextMap(context: java.util.Map[String, String]) {
    if (context == null) {
      MDC.clear()
    } else {
      MDC.setContextMap(context)
    }
  }

  override def reportFailure(cause: Throwable): Unit = delegate.reportFailure(cause)
}

/**
  * Use this instead of the default Play Action object.
  */
object Action extends ActionBuilder[Request] {

  override protected def executionContext: ExecutionContext = {
    new MDCPropagatingExecutionContext(MDC.getCopyOfContextMap, Execution.defaultContext)
  }

  def invokeBlock[A](request: Request[A], block: (Request[A]) => Future[Result]): Future[Result] = {
    block(request)
  }
}
