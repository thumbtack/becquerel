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

import com.sksamuel.elastic4s.Hit
import com.sksamuel.elastic4s.http.index.mappings.IndexMappings
import com.thumbtack.becquerel.datasources.{ODataStrings, TableMapper}
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.apache.olingo.commons.api.edm.provider.{CsdlEntitySet, CsdlEntityType, CsdlPropertyRef}

import scala.collection.JavaConverters._

object EsTableMapper {
  def apply(
    namespace: String
  )(
    indexMappings: IndexMappings
  ): TableMapper[AnyRef, Hit] = {

    val indexName = indexMappings.index

    // We expect there to be only one mapping per index.
    assert(indexMappings.mappings.size == 1)
    val (mappingName, mapping) = indexMappings.mappings.head

    val tableIDParts = Seq(indexName, mappingName)

    val tableName = ODataStrings.sanitizeName(indexName)

    val entitySet = new CsdlEntitySet()
      .setName(tableName)
      .setType(new FullQualifiedName(namespace, tableName))

    val fieldMappers = EsFieldMapper.idMapper +: mapping
      .asInstanceOf[Map[String, Map[String, Any]]]
      .map((EsFieldMapper(namespace, tableName) _).tupled)
      .toIndexedSeq

    // Elasticsearch document IDs are always returned by search hits.
    // TODO: this implementation has the annoying side effect of adding the ES _id to every table schema.
    // TODO: we could use mapper field _meta to mark which one is supposed to be the table's PK.
    val idFields = Seq(EsFieldMapper.idMapper.name)

    val entityType = new CsdlEntityType()
      .setName(tableName)
      .setProperties(
        fieldMappers
          .map(_.property)
          .asJava)
      .setKey(
        Seq(new CsdlPropertyRef().setName(EsFieldMapper.idMapper.property.getName))
          .asJava)

    val rowMapper = EsRowMapper(
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
