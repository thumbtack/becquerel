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

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.control.NonFatal

import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import org.apache.http.HttpHost
import org.apache.http.impl.nio.client.{CloseableHttpAsyncClient, HttpAsyncClientBuilder}
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback
import org.elasticsearch.client.{ResponseException, RestClient}
import org.scalatest.FunSuite
import play.api.http.Status

import com.thumbtack.becquerel.datasources.elasticsearch.mocks.{MockHttpAsyncClient, MockScheduler}

class EsRetryHttpClientTest extends FunSuite {

  test("execute without retries") {
    val mockHttpAsyncClient = new MockHttpAsyncClient(succeedAfter = Int.MaxValue)
    val esClient = newEsClient(mockHttpAsyncClient)

    val exception: Option[Throwable] = try {
      esClient
        .execute {
          indexExists("foo")
        }
        .await()
      None
    } catch {
      case NonFatal(e) => Some(e)
    }

    assert(mockHttpAsyncClient.numRequests === 1)
    assert(exception.exists(_.isInstanceOf[ResponseException]))
  }

  test("execute with default retries until giving up") {
    val mockHttpAsyncClient = new MockHttpAsyncClient(succeedAfter = Int.MaxValue)
    val mockScheduler = new MockScheduler()
    val esClient = new EsRetryHttpClient(
      newEsClient(mockHttpAsyncClient),
      mockScheduler,
      initialWait = 1.second,
      maxWait = 8.seconds,
      maxAttempts = 5,
      statusCodes = Set(Status.TOO_MANY_REQUESTS)
    )

    val exception: Option[Throwable] = try {
      esClient
        .execute {
          indexExists("foo")
        }
        .await()
      None
    } catch {
      case NonFatal(e) => Some(e)
    }

    // Should still fail but only after retrying the expected number of times.
    assert(mockHttpAsyncClient.numRequests === 5)
    assert(mockScheduler.delays.size === 4)
    assert(mockScheduler.delays.sum === 15.seconds)
    assert(exception.exists(_.isInstanceOf[ResponseException]))
  }

  test("execute past max wait until giving up") {
    val mockHttpAsyncClient = new MockHttpAsyncClient(succeedAfter = Int.MaxValue)
    val mockScheduler = new MockScheduler()
    val esClient = new EsRetryHttpClient(
      newEsClient(mockHttpAsyncClient),
      mockScheduler,
      initialWait = 1.second,
      maxWait = 8.seconds,
      maxAttempts = 6,
      statusCodes = Set(Status.TOO_MANY_REQUESTS)
    )

    val exception: Option[Throwable] = try {
      esClient
        .execute {
          indexExists("foo")
        }
        .await()
      None
    } catch {
      case NonFatal(e) => Some(e)
    }

    // Should still fail but only after retrying the expected number of times.
    assert(mockHttpAsyncClient.numRequests === 6)
    assert(mockScheduler.delays.size === 5)
    assert(mockScheduler.delays.sum === 23.seconds)
    assert(exception.exists(_.isInstanceOf[ResponseException]))
  }

  test("execute with default retries until success") {
    val mockHttpAsyncClient = new MockHttpAsyncClient(succeedAfter = 2)
    val mockScheduler = new MockScheduler()
    val esClient = new EsRetryHttpClient(
      newEsClient(mockHttpAsyncClient),
      mockScheduler,
      initialWait = 1.second,
      maxWait = 8.seconds,
      maxAttempts = 5,
      statusCodes = Set(Status.TOO_MANY_REQUESTS)
    )

    val exception: Option[Throwable] = try {
      esClient
        .execute {
          indexExists("foo")
        }
        .await()
      None
    } catch {
      case NonFatal(e) => Some(e)
    }

    // Should succeed on the third request.
    assert(mockHttpAsyncClient.numRequests === 3)
    assert(mockScheduler.delays.size === 2)
    assert(mockScheduler.delays.sum === 3.seconds)
    assert(exception.isEmpty)
  }

  /**
    * Create an ES client backed by a mocked HTTP client.
    */
  def newEsClient(mockHttpAsyncClient: MockHttpAsyncClient): HttpClient = {
    HttpClient.fromRestClient(
      RestClient
        .builder(new HttpHost("elasticsearch.example.com"))
        .setHttpClientConfigCallback(new HttpClientConfigCallback {
          override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
            new HttpAsyncClientBuilder() {
              override def build(): CloseableHttpAsyncClient = mockHttpAsyncClient
            }
          }
        })
        .build()
    )
  }

  /**
    * Enough numeric methods to use `sum`.
    */
  //noinspection NotImplementedCode
  implicit val numericDuration: Numeric[Duration] = new Numeric[Duration] {
    override def plus(x: Duration, y: Duration): Duration = x.plus(y)
    override def minus(x: Duration, y: Duration): Duration = x.minus(y)
    override def negate(x: Duration): Duration = x.neg()
    override def compare(x: Duration, y: Duration): Int = x.compare(y)
    override def zero: Duration = Duration.Zero

    override def times(x: Duration, y: Duration): Duration = ???
    override def fromInt(x: Int): Duration = ???
    override def toInt(x: Duration): Int = ???
    override def toLong(x: Duration): Long = ???
    override def toFloat(x: Duration): Float = ???
    override def toDouble(x: Duration): Double = ???
    override def one: Duration = ???
  }
}
