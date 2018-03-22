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

package com.thumbtack.becquerel.datasources.sql

import scala.collection.JavaConverters._

import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.{SqlIdentifier, SqlNode, SqlNodeList}
import org.apache.olingo.server.api.uri.UriResourceProperty
import org.apache.olingo.server.api.uri.queryoption.{SelectItem, SelectOption}

import com.thumbtack.becquerel.datasources.{FieldMapper, RowMapper, TableMapper}
import com.thumbtack.becquerel.util.BecquerelException

/**
  * Translate a subset of OData `$$select` expressions to a Calcite SQL AST and a row mapper.
  */
//noinspection NotImplementedCode
object SqlSelectCompiler {

  def compile[Column](
    tableMapper: TableMapper[Column, Seq[Column]],
    select: Option[SelectOption]
  ): (RowMapper[Column, Seq[Column]], SqlNodeList) = {

    select
      .map(_.getSelectItems.asScala)
      .map(_.map(resolve(tableMapper)))
      .map { fields =>

        val rowMapper = tableMapper.rowMapper.withFields(fields.map(_._1))

        val selectSqlNodes = new SqlNodeList(
          fields.map(_._2).asJava,
          SqlParserPos.ZERO)

        (rowMapper, selectSqlNodes)
      }
      .getOrElse((tableMapper.rowMapper, star()))
  }

  /**
    * Select all columns in this table.
    */
  def star(): SqlNodeList = {
    new SqlNodeList(Seq(SqlIdentifier.star(SqlParserPos.ZERO)).asJava, SqlParserPos.ZERO)
  }

  def resolve[Column](
    tableMapper: TableMapper[Column, Seq[Column]]
  )(
    selectItem: SelectItem
  ): (FieldMapper[Column], SqlNode) = {

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
          fieldMapper -> new SqlIdentifier(fieldMapper.name, SqlParserPos.ZERO)

        // TODO: support nested properties
        //case Seq(head: UriResourceProperty, tail @ _*) =>

        case _ => ???
      }
    }
  }
}
