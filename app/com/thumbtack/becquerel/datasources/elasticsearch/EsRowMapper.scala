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

import java.net.URI

import com.sksamuel.elastic4s.Hit
import com.thumbtack.becquerel.datasources.{FieldMapper, ODataStrings, RowMapper}
import org.apache.olingo.commons.api.data.{Entity, Property}

case class EsRowMapper(
  fieldMappers: Seq[FieldMapper[AnyRef]],
  idFields: Seq[String],
  entitySetName: String
) extends RowMapper[AnyRef, Hit] {
  override protected def rowToProperties(row: Hit): Seq[Property] = {
    val source = row.sourceAsMap
    EsFieldMapper.idMapper(row.id) +: fieldMappers
      .filter(_ != EsFieldMapper.idMapper)
      .map { fieldMapper =>
        // ES doesn't return values at all for fields that are null.
        fieldMapper(source.get(fieldMapper.name).orNull)
      }
  }

  override def withFields(fieldMappers: Seq[FieldMapper[AnyRef]]): RowMapper[AnyRef, Hit] = {
    copy(fieldMappers = fieldMappers)
  }

  /**
    * Generate a relative URI containing the PK for this entity.
    */
  override protected def generateID(entity: Entity, row: Hit): URI = {
    val keyPredicatesText = ODataStrings.unparseStringLiteral(row.id)

    new URI(
      null,
      null,
      s"$entitySetName($keyPredicatesText)",
      null
    )
  }
}
