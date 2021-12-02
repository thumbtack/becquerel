/*
 *    Copyright 2017–2018 Thumbtack
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

import scala.collection.JavaConverters._

import com.sksamuel.elastic4s.Hit
import com.sksamuel.elastic4s.requests.common.FetchSourceContext
import org.apache.olingo.server.api.uri.UriResourceProperty
import org.apache.olingo.server.api.uri.queryoption.{SelectItem, SelectOption}

import com.thumbtack.becquerel.datasources.{FieldMapper, RowMapper, TableMapper}
import com.thumbtack.becquerel.util.BecquerelException

/**
  * Filter the _source field on results so that a search only returns named top-level properties.
  *
  * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-source-filtering.html
  */
//noinspection NotImplementedCode
object EsSelectCompiler {

  def compile(
    tableMapper: TableMapper[AnyRef, Hit],
    select: Option[SelectOption]
  ): (RowMapper[AnyRef, Hit], Option[FetchSourceContext]) = {

    select
      .map(_.getSelectItems.asScala)
      .map(_.map(resolve(tableMapper)))
      .map { fields =>
        val rowMapper = tableMapper.rowMapper.withFields(fields.map(_._1))
        val sourceFilter = new FetchSourceContext(true, fields.map(_._2).toSet, Set.empty[String])
        (rowMapper, Some(sourceFilter): Option[FetchSourceContext])
      }
      .getOrElse((tableMapper.rowMapper, None)) // Don't filter the source.
  }

  def resolve(
    tableMapper: TableMapper[AnyRef, Hit]
  )(
    selectItem: SelectItem
  ): (FieldMapper[AnyRef], String) = {

    // We don't support these.
    if (selectItem.isAllOperationsInSchema) { ??? }
    if (selectItem.getStartTypeFilter != null) { ??? }

    if (selectItem.isStar) {
      ??? // TODO: we could support this
    } else {
      selectItem.getResourcePath.getUriResourceParts.asScala match {
        case Seq(head: UriResourceProperty) =>
          val fieldMapper = tableMapper.rowMapper.fieldMappers
            .find(_.property.getName == head.getProperty.getName)
            .getOrElse {
              throw new BecquerelException(s"Couldn't resolve property: ${head.getProperty.getName}")
            }
          fieldMapper -> fieldMapper.name

        // TODO: support nested properties
        //case Seq(head: UriResourceProperty, tail @ _*) =>

        case _ => ???
      }
    }
  }
}
