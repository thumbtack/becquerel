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

package com.thumbtack.becquerel.datasources.elasticsearch

import com.sksamuel.elastic4s.requests.common.FetchSourceContext
import com.sksamuel.elastic4s.requests.searches.SearchRequest
import com.sksamuel.elastic4s.requests.searches.queries.{BoolQuery, Query}
import com.sksamuel.elastic4s.{Hit, Indexes, IndexesAndTypes}
import com.thumbtack.becquerel.datasources.{RowMapper, TableMapper}
import org.apache.olingo.server.api.uri.queryoption._


object EsQueryCompiler {
  /**
    * Translate an OData query to an ES query and a row mapper.
    */
  def compile(
    tableMapper: TableMapper[AnyRef, Hit],
    filter: Option[FilterOption],
    search: Option[SearchOption],
    select: Option[SelectOption],
    orderBy: Option[OrderByOption],
    top: Option[TopOption],
    skip: Option[SkipOption]
  ): (RowMapper[AnyRef, Hit], SearchRequest) = {

    val (rowMapper: RowMapper[AnyRef, Hit], fetchContext: Option[FetchSourceContext]) = EsSelectCompiler
      .compile(tableMapper, select)

    val indexesAndTypes: IndexesAndTypes = tableMapper.tableIDParts match {
      case Seq(index, mapping) => IndexesAndTypes(index, mapping)
    }

    val indexes: Indexes = tableMapper.tableIDParts match {
      case Seq(index, _) => Indexes(Seq(index))
    }

    val filterQuery: Option[Query] = EsExpressionCompiler.compileFilter(filter)
    val searchQuery: Option[Query] = EsExpressionCompiler.compileSearch(search)
    val query: Option[Query] = Seq(
      filterQuery,
      searchQuery
    )
      .flatten match {
      case Seq() => None
      case Seq(single) => Some(single)
      case many => Some(BoolQuery(must = many))
    }

    val searchRequest = SearchRequest(
      indexes = indexes,
      query = query,
      sorts = EsExpressionCompiler.compileOrderBy(orderBy),
      fetchContext = fetchContext,
      from = skip.map(_.getValue),
      size = top.map(_.getValue)
    )

    (rowMapper, searchRequest)
  }
}
