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

package com.thumbtack.becquerel.datasources.sql

import com.thumbtack.becquerel.datasources.{DataSourceService, RowMapper, TableMapper}
import org.apache.calcite.sql.SqlDialect
import org.apache.olingo.server.api.uri.queryoption._

import com.thumbtack.becquerel.datasources.bigquery.BqStrings

/**
  * OData service backed by a SQL provider.
  *
  * @tparam Column Column type.
  * @tparam Schema Schema type.
  */
trait SqlService[Column, Schema] extends DataSourceService[Column, Seq[Column], Schema, String] {
  /**
    * @return Calcite SQL dialect for this service.
    */
  protected def dialect: SqlDialect

  /**
    * Expression compiler for this service.
    */
  protected def expressionCompiler: SqlExpressionCompiler

  protected def compile(
    runID: Option[String],
    tableMapper: TableMapper[Column, Seq[Column]],
    filter: Option[FilterOption],
    search: Option[SearchOption],
    select: Option[SelectOption],
    orderBy: Option[OrderByOption],
    top: Option[TopOption],
    skip: Option[SkipOption]
  ): (RowMapper[Column, Seq[Column]], String) = {
    val (rowMapper, ast) = SqlQueryCompiler.compile(
      expressionCompiler,
      tableMapper,
      filter,
      search,
      select,
      orderBy,
      top,
      skip
    )
    val sql = ast.toSqlString(dialect).getSql
    // TODO: obviously only safe for BQ; move comment node generator to expression compiler.
    val decoratedSQL = runID match {
      case Some(safeRunID) if BqStrings.isSingleLineCommentSafe(safeRunID) =>
        s"-- run_id: $safeRunID\n$sql"
      case _ => sql
    }
    (rowMapper, decoratedSQL)
  }
}
