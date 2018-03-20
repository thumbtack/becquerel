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

import org.apache.http.HttpVersion
import org.apache.http.concurrent.FutureCallback
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient
import org.apache.http.message.{BasicHttpResponse, BasicStatusLine}
import org.apache.http.nio.protocol.{HttpAsyncRequestProducer, HttpAsyncResponseConsumer}
import org.apache.http.protocol.HttpContext
import play.api.http.Status

/**
  * Mock HTTP client that does nothing other than return 200 or 429 and count requests.
  */
class MockHttpAsyncClient(succeedAfter: Int) extends CloseableHttpAsyncClient {
  override def start(): Unit = ()
  override def isRunning: Boolean = true
  override def close(): Unit = ()

  var numRequests: Int = 0

  override def execute[T](
    requestProducer: HttpAsyncRequestProducer,
    responseConsumer: HttpAsyncResponseConsumer[T],
    context: HttpContext,
    callback: FutureCallback[T]
  ): java.util.concurrent.Future[T] = {
    numRequests += 1
    val response = new BasicHttpResponse(
      new BasicStatusLine(
        HttpVersion.HTTP_1_1,
        if (numRequests > succeedAfter) {
          Status.OK
        } else {
          Status.TOO_MANY_REQUESTS
        },
        null
      )
    )
    responseConsumer.responseReceived(response)
    responseConsumer.responseCompleted(context)
    callback.completed(response.asInstanceOf[T])
    null
  }
}
