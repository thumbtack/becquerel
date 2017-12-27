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

package com.thumbtack.becquerel.datasources.bigquery

import com.google.auth.Credentials
import com.google.cloud.bigquery.{BigQuery, BigQueryOptions}

/**
  * Construct a BigQuery service from common options.
  *
  * BigQueryOptions/BigQueryOptions.Builder can't be mocked easily due to private constructors.
  */
trait BqBuilder {
  def apply(projectID: Option[String], credentials: Option[Credentials]): BigQuery
}

/**
  * Normal implementation of BqBuilder.
  */
class GoogleBqBuilder extends BqBuilder {
  override def apply(projectID: Option[String], credentials: Option[Credentials]): BigQuery = {
    val bigQueryOptionsBuilder = BigQueryOptions.newBuilder()
    projectID.foreach(bigQueryOptionsBuilder.setProjectId)
    credentials.foreach(bigQueryOptionsBuilder.setCredentials)
    bigQueryOptionsBuilder.build().getService
  }
}
