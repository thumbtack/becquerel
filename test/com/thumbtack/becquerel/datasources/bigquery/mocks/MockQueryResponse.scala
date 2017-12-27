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

package com.thumbtack.becquerel.datasources.bigquery.mocks

import java.lang.reflect.Method

import com.google.cloud.bigquery._

/**
  * [[QueryResponse]] doesn't have a public constructor. Fake it.
  */
object MockQueryResponse {
  val newBuilder: Method = classOf[QueryResponse].getDeclaredMethod("newBuilder")
  newBuilder.setAccessible(true)

  val builderClass: Class[_] = Class.forName("com.google.cloud.bigquery.QueryResponse$Builder")

  val setResult: Method = builderClass.getDeclaredMethod("setResult", classOf[QueryResult])
  setResult.setAccessible(true)

  val setJobCompleted: Method = builderClass.getDeclaredMethod("setJobCompleted", classOf[Boolean])
  setJobCompleted.setAccessible(true)

  val build: Method = builderClass.getDeclaredMethod("build")
  build.setAccessible(true)

  def apply(queryResult: QueryResult): QueryResponse = {
    val builder = newBuilder.invoke(null)
    setResult.invoke(builder, queryResult)
    val jobCompleted: java.lang.Boolean = true
    setJobCompleted.invoke(builder, jobCompleted)
    build.invoke(builder).asInstanceOf[QueryResponse]
  }
}
