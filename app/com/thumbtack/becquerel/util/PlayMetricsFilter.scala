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

import javax.inject.Inject

import akka.stream.Materializer
import com.kenshoo.play.metrics.{Metrics, MetricsFilterImpl}

/**
  * Shorten name for metrics on HTTP requests.
  */
class PlayMetricsFilter @Inject() (metrics: Metrics)(override implicit val mat: Materializer) extends MetricsFilterImpl(metrics)(mat) {
  override def labelPrefix: String = "play"
}
