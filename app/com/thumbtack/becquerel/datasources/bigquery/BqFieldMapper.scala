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

import com.google.cloud.bigquery.Field.Mode
import com.google.cloud.bigquery.{Field, FieldValue, LegacySQLTypeName}
import com.thumbtack.becquerel.datasources.{FieldMapper, ODataStrings, ValueMapper}
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.apache.olingo.commons.api.edm.provider.{CsdlComplexType, CsdlProperty}

import scala.collection.JavaConverters._

/**
  * @param name Name of BigQuery column.
  */
case class BqFieldMapper(
  name: String,
  property: CsdlProperty,
  valueMapper: ValueMapper[FieldValue]
) extends FieldMapper[FieldValue]

object BqFieldMapper {
  def apply(namespace: String, parentTypeName: String)(field: Field): BqFieldMapper = {
    val bqName = field.getName
    val odataName = ODataStrings.sanitizeName(field.getName)

    val valueMapper = field.getType.getValue match {
      case LegacySQLTypeName.RECORD =>
        val thisTypeName = Seq(parentTypeName, odataName)
          .mkString(ODataStrings.idSep)

        val typeFQN = new FullQualifiedName(namespace, thisTypeName)

        val fieldMappers = field.getFields.asScala
          .map(BqFieldMapper(namespace, thisTypeName))
        assert(fieldMappers.nonEmpty)

        val complexType = new CsdlComplexType()
          .setName(thisTypeName)
          .setProperties(
            fieldMappers
              .map(_.property)
              .asJava)

        BqComplexMapper(typeFQN, complexType, fieldMappers)

      case LegacySQLTypeName.BOOLEAN => BqBooleanMapper
      case LegacySQLTypeName.BYTES => BqBytesMapper
      case LegacySQLTypeName.DATE => BqDateMapper
      case LegacySQLTypeName.DATETIME => BqDateTimeMapper
      case LegacySQLTypeName.FLOAT => BqFloatMapper
      case LegacySQLTypeName.INTEGER => BqIntegerMapper
      case LegacySQLTypeName.STRING => BqStringMapper
      case LegacySQLTypeName.TIME => BqTimeMapper
      case LegacySQLTypeName.TIMESTAMP => BqTimestampMapper

      case unknown =>
        throw new NotImplementedError(s"BigQuery type $unknown has no native CSDL equivalent")
    }

    val precision: Option[Int] = field.getType.getValue match {
      // BigQuery supports microsecond time resolution.
      case LegacySQLTypeName.DATETIME |
           LegacySQLTypeName.TIME |
           LegacySQLTypeName.TIMESTAMP => Some(6)
      case _ => None
    }

    val mode = Option(field.getMode).getOrElse(Mode.NULLABLE)

    val wrappedValueMapper = mode match {
      case Mode.NULLABLE => BqNullableMapper(valueMapper)
      case Mode.REPEATED => BqRepeatedMapper(valueMapper)
      case Mode.REQUIRED => valueMapper

      case unknown =>
        throw new NotImplementedError(s"BigQuery mode $unknown has no native CSDL equivalent")
    }

    val property = new CsdlProperty()
      .setName(odataName)
      .setCollection(mode == Mode.REPEATED)
      .setNullable(mode == Mode.NULLABLE)
      .setType(wrappedValueMapper.typeFQN)
      .setPrecision(precision.map(Integer.valueOf).orNull)

    BqFieldMapper(
      bqName,
      property,
      wrappedValueMapper)
  }
}
