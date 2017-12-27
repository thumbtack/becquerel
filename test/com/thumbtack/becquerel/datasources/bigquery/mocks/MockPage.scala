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

import com.google.cloud.Page

import scala.collection.JavaConverters._

/**
  * Exposes a collection as a single page.
  */
class MockPage[T](items: Seq[T]) extends Page[T] {
  override def getNextPage: Page[T] = null

  override def getValues: java.lang.Iterable[T] = items.asJava

  override def getNextPageCursor: String = null

  override def iterateAll(): java.util.Iterator[T] = items.toIterator.asJava

  override def nextPage(): Page[T] = getNextPage

  override def nextPageCursor(): String = getNextPageCursor

  override def values(): java.lang.Iterable[T] = getValues
}
