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

import com.thumbtack.becquerel.datasources.{FieldMapper, RowMapper}
import org.apache.olingo.commons.api.data.Property

/**
  * SQL rows always have their columns in a fixed order.
  */
case class SqlRowMapper[Column](
  fieldMappers: Seq[FieldMapper[Column]],
  idFields: Seq[String],
  entitySetName: String
) extends RowMapper[Column, Seq[Column]] {
  override protected def rowToProperties(row: Seq[Column]): Seq[Property] = {
    (fieldMappers, row)
      .zipped
      .map(_(_))
  }

  override def withFields(fieldMappers: Seq[FieldMapper[Column]]): RowMapper[Column, Seq[Column]] = {
    copy(fieldMappers = fieldMappers)
  }
}
