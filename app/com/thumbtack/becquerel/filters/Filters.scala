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

import javax.inject.Inject

import com.kenshoo.play.metrics.MetricsFilter
import play.api.http.DefaultHttpFilters
import play.filters.gzip.GzipFilter

/**
  * Configure HTTP request/response filters applied to all Play requests.
  *
  * @param gzipFilter See https://www.playframework.com/documentation/2.5.x/GzipEncoding
  */
class Filters @Inject() (
  runIDFilter: RunIDFilter,
  gzipFilter: GzipFilter,
  loggingFilter: LoggingFilter,
  metricsFilter: MetricsFilter
) extends DefaultHttpFilters(
  runIDFilter,
  gzipFilter,
  loggingFilter,
  metricsFilter
)
