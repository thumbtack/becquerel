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

package com.thumbtack.becquerel.datasources.jdbc

import java.sql.ResultSet

class JdbcResultSetIterator(private val resultSet: ResultSet) extends Iterator[IndexedSeq[AnyRef]] with AutoCloseable {

  val numColumns: Int = resultSet.getMetaData.getColumnCount

  private var resultSetNextSucceeded = resultSet.next()

  override def hasNext: Boolean = resultSetNextSucceeded

  override def next(): IndexedSeq[AnyRef] = {
    val row = (1 to numColumns).map(resultSet.getObject)
    resultSetNextSucceeded = resultSet.next()
    row
  }

  override def close(): Unit = resultSet.close()
}

object JdbcResultSetIterator {
  implicit class IterableResultSet(resultSet: ResultSet) extends Iterable[IndexedSeq[AnyRef]] {
    override def iterator: Iterator[IndexedSeq[AnyRef]] = new JdbcResultSetIterator(resultSet)
  }
}
