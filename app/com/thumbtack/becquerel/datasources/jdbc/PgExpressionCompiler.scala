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

import com.thumbtack.becquerel.datasources.sql.SqlExpressionCompiler
import org.apache.calcite.sql.{SqlIdentifier, SqlNode}
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind

object PgExpressionCompiler extends SqlExpressionCompiler {
  override def dialectContains(haystack: SqlNode, needle: SqlNode): SqlNode = ???

  override def dialectContainsIgnoreCase(haystack: SqlNode, needle: SqlNode): SqlNode = ???

  override def dialectStartsWith(haystack: SqlNode, prefix: SqlNode): SqlNode = ???

  override def dialectEndsWith(haystack: SqlNode, suffix: SqlNode): SqlNode = ???

  override def dialectPrimitiveType(edmPrimitiveTypeKind: EdmPrimitiveTypeKind): SqlIdentifier = ???
}
