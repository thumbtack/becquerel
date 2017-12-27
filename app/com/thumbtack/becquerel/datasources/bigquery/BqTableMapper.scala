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

package com.thumbtack.becquerel.datasources.bigquery

import com.google.cloud.bigquery.{FieldValue, TableDefinition, TableId}
import com.thumbtack.becquerel.datasources.sql.SqlRowMapper
import com.thumbtack.becquerel.datasources.{ODataStrings, TableMapper}
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.apache.olingo.commons.api.edm.provider.{CsdlEntitySet, CsdlEntityType, CsdlPropertyRef}

import scala.collection.JavaConverters._

object BqTableMapper {
  def apply(
    namespace: String,
    omitProjectID: Boolean
  )(
    tableId: TableId,
    tableDefinition: TableDefinition
  ): TableMapper[FieldValue, Seq[FieldValue]] = {

    val tableIDParts = Seq(tableId.getProject, tableId.getDataset, tableId.getTable)

    val tableName = (
      if (omitProjectID) {
        Seq(tableId.getDataset, tableId.getTable)
      } else {
        Seq(tableId.getProject, tableId.getDataset, tableId.getTable)
      }
    )
      .map(ODataStrings.sanitizeName)
      .mkString(ODataStrings.idSep)

    val entitySet = new CsdlEntitySet()
      .setName(tableName)
      .setType(new FullQualifiedName(namespace, tableName))

    val fields = tableDefinition
      .getSchema
      .getFields
      .asScala

    val fieldMappers = fields
      .map(BqFieldMapper(namespace, tableName))

    // TODO: Assumes the first column is a simple primary key.
    val idFields = Seq(fieldMappers.head.name)

    val entityType = new CsdlEntityType()
      .setName(tableName)
      .setProperties(
        fieldMappers
          .map(_.property)
          .asJava)
      .setKey(
        idFields
          .map {
            idField =>
              val fieldMapper = fieldMappers.find(_.name == idField).get
              new CsdlPropertyRef().setName(fieldMapper.property.getName)
          }
          .asJava)

    val rowMapper = SqlRowMapper(
      fieldMappers,
      idFields,
      entitySet.getName
    )

    TableMapper(
      tableIDParts,
      entitySet,
      entityType,
      rowMapper,
      idFields
    )
  }
}
