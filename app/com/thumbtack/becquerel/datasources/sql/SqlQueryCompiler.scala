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

import com.thumbtack.becquerel.datasources.{RowMapper, TableMapper}
import com.thumbtack.becquerel.util.CalciteExtensions._
import org.apache.calcite.sql._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.olingo.server.api.uri.queryoption._

import scala.collection.JavaConverters._

object SqlQueryCompiler {
  /**
    * Translate an OData query to a SQL query and a row mapper.
    */
  def compile[Column](
    expressionCompiler: SqlExpressionCompiler,
    tableMapper: TableMapper[Column, Seq[Column]],
    filter: Option[FilterOption],
    search: Option[SearchOption],
    select: Option[SelectOption],
    orderBy: Option[OrderByOption],
    top: Option[TopOption],
    skip: Option[SkipOption]
  ): (RowMapper[Column, Seq[Column]], SqlNode) = {

    val whereFilter: Option[SqlNode] = expressionCompiler.compileFilter(filter)
    val whereSearch: Option[SqlNode] = expressionCompiler.compileSearch(tableMapper, search)
    // If both are used, results should match both, so we AND the queries together.
    val where: SqlNode = Seq(
      whereFilter,
      whereSearch
    )
      .flatten
      .reduceOption(SqlStdOperatorTable.AND(_, _))
      .orNull

    val (rowMapper: RowMapper[Column, Seq[Column]], selectList: SqlNodeList) = SqlSelectCompiler
      .compile(tableMapper, select)

    val from: SqlIdentifier = new SqlIdentifier(
      tableMapper.tableIDParts.asJava,
      SqlParserPos.ZERO)

    val orderByList: SqlNodeList = expressionCompiler
      .compileOrderBy(orderBy)
      .orNull

    val fetch: SqlNode = top
      .map(top => SqlLiteral.createExactNumeric(top.getValue.toString, SqlParserPos.ZERO))
      .orNull

    val offset: SqlNode = skip
      .map(skip => SqlLiteral.createExactNumeric(skip.getValue.toString, SqlParserPos.ZERO))
      .orNull

    // Currently unused.
    val keywordList: SqlNodeList = null
    val groupBy: SqlNodeList = null
    val having: SqlNode = null
    val windowDecls: SqlNodeList = null

    val sqlSelect = new SqlSelect(
      SqlParserPos.ZERO,
      keywordList,
      selectList,
      from,
      where,
      groupBy,
      having,
      windowDecls,
      orderByList,
      offset,
      fetch)

    (rowMapper, sqlSelect)
  }
}
