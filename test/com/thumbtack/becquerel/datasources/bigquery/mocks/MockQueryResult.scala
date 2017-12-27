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

import scala.collection.JavaConverters._

/**
  * [[QueryResult]] doesn't have a public constructor. Fake it.
  */
object MockQueryResult {
  val newBuilder: Method = classOf[QueryResult].getDeclaredMethod("newBuilder")
  newBuilder.setAccessible(true)

  val builderClass: Class[_] = Class.forName("com.google.cloud.bigquery.QueryResult$Builder")

  val setResults: Method = builderClass.getDeclaredMethod("setResults", classOf[java.lang.Iterable[java.util.List[FieldValue]]])
  setResults.setAccessible(true)

  val build: Method = builderClass.getDeclaredMethod("build")
  build.setAccessible(true)

  def apply(rows: Seq[Seq[FieldValue]]): QueryResult = {
    val builder = newBuilder.invoke(null)
    val results: java.lang.Iterable[java.util.List[FieldValue]] = rows.map(_.asJava).asJava
    setResults.invoke(builder, results)
    build.invoke(builder).asInstanceOf[QueryResult]
  }
}
