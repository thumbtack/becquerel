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

package com.thumbtack.becquerel.datasources.jdbc

import java.sql.JDBCType

import com.thumbtack.becquerel.datasources.{FieldMapper, ODataStrings, ValueMapper}
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.provider.CsdlProperty

import scala.util.{Failure, Success, Try}

/**
  * @param name Name of column in SQL DB.
  */
case class JdbcFieldMapper(
  name: String,
  property: CsdlProperty,
  valueMapper: ValueMapper[AnyRef]
) extends FieldMapper[AnyRef]

object JdbcFieldMapper {
  def apply(namespace: String, parentTypeName: String)(columnDefinition: JdbcColumnDefinition): JdbcFieldMapper = {

    val colName = columnDefinition.name
    val odataName = ODataStrings.sanitizeName(columnDefinition.name)

    val tryJDBCType = Try(JDBCType.valueOf(columnDefinition.dataType))

    val valueMapper = tryJDBCType match {
      case Success(jdbcType) => jdbcType match {
        // See http://download.oracle.com/otn-pub/jcp/jdbc-4_1-mrel-spec/jdbc4.1-fr-spec.pdf
        // (Appendix B, Table B-1) for how Java maps JDBC types to Java objects.

        // Integer types.
        case JDBCType.BIT |
             JDBCType.BOOLEAN => JdbcPrimitiveMapper(EdmPrimitiveTypeKind.Boolean)
        case JDBCType.TINYINT => JdbcPrimitiveMapper(EdmPrimitiveTypeKind.SByte)
        case JDBCType.SMALLINT => JdbcPrimitiveMapper(EdmPrimitiveTypeKind.Int16)
        case JDBCType.INTEGER => JdbcPrimitiveMapper(EdmPrimitiveTypeKind.Int32)
        case JDBCType.BIGINT => JdbcPrimitiveMapper(EdmPrimitiveTypeKind.Int64)

        // Floating point types.
        case JDBCType.REAL => JdbcPrimitiveMapper(EdmPrimitiveTypeKind.Single)
        case JDBCType.FLOAT |
             JDBCType.DOUBLE => JdbcPrimitiveMapper(EdmPrimitiveTypeKind.Double)

        // Decimal types.
        case JDBCType.DECIMAL |
             JDBCType.NUMERIC => JdbcPrimitiveMapper(EdmPrimitiveTypeKind.Decimal)

        // String types.
        case JDBCType.CHAR |
             JDBCType.VARCHAR |
             JDBCType.LONGVARCHAR |
             JDBCType.NCHAR |
             JDBCType.NVARCHAR |
             JDBCType.LONGNVARCHAR => JdbcPrimitiveMapper(EdmPrimitiveTypeKind.String)

        // Binary types.
        case JDBCType.BINARY |
             JDBCType.VARBINARY |
             JDBCType.LONGVARBINARY => JdbcPrimitiveMapper(EdmPrimitiveTypeKind.Binary)

        // Temporal types.
        case JDBCType.DATE => JdbcPrimitiveMapper(EdmPrimitiveTypeKind.TimeOfDay)
        case JDBCType.TIME => JdbcPrimitiveMapper(EdmPrimitiveTypeKind.Date)
        case JDBCType.TIMESTAMP_WITH_TIMEZONE => JdbcPrimitiveMapper(EdmPrimitiveTypeKind.DateTimeOffset)

        case unknown =>
          throw new NotImplementedError(s"$colName: JDBC type $unknown has no native CSDL equivalent or hasn't been mapped yet")
      }

      case Failure(_) =>
        throw new NotImplementedError(s"$colName: ${columnDefinition.dataType} is not a JDBC generic type number, and is probably dialect-specific")
    }

    // https://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part3-csdl/odata-v4.0-errata03-os-part3-csdl-complete.html#_Toc453752529
    // Assume that if we don't know if it's nullable, we might still have to deal with nulls.
    val nullable = columnDefinition.nullable.getOrElse(true)

    val wrappedValueMapper = if (nullable) {
      JdbcNullableMapper(valueMapper)
    } else {
      valueMapper
    }

    // https://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part3-csdl/odata-v4.0-errata03-os-part3-csdl-complete.html#_Toc453752530
    // String/binary/stream types only.
    val maxLength = tryJDBCType match {
      case Success(jdbcType) => jdbcType match {

        case JDBCType.CHAR |
             JDBCType.VARCHAR |
             JDBCType.LONGVARCHAR |
             JDBCType.NCHAR |
             JDBCType.NVARCHAR |
             JDBCType.LONGNVARCHAR |
             JDBCType.BINARY |
             JDBCType.VARBINARY |
             JDBCType.LONGVARBINARY => columnDefinition.size

        case _ => None
      }
      case _ => None
    }

    // https://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part3-csdl/odata-v4.0-errata03-os-part3-csdl-complete.html#_Toc453752531
    // Decimal types: max significant decimal digits.
    // Temporal types: max digits to the right of the decimal point for the seconds field.
    val precision = tryJDBCType match {
      case Success(jdbcType) => jdbcType match {

        case JDBCType.DECIMAL |
             JDBCType.NUMERIC => columnDefinition.size

        case JDBCType.TIME |
             JDBCType.TIMESTAMP_WITH_TIMEZONE => columnDefinition.decimalDigits

        case _ => None
      }
      case _ => None
    }

    // https://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part3-csdl/odata-v4.0-errata03-os-part3-csdl-complete.html#_Toc453752532
    // Decimal types: max digits to the right of the decimal point.
    val scale = tryJDBCType match {
      case Success(jdbcType) => jdbcType match {

        case JDBCType.DECIMAL |
             JDBCType.NUMERIC => columnDefinition.decimalDigits

        case _ => None
      }
      case _ => None
    }

    val property = new CsdlProperty()
      .setName(odataName)
      .setCollection(false)
      .setNullable(nullable)
      .setType(wrappedValueMapper.typeFQN)
      .setMaxLength(maxLength.map(Int.box).orNull)
      .setPrecision(precision.map(Int.box).orNull)
      .setScale(scale.map(Int.box).orNull)

    JdbcFieldMapper(
      colName,
      property,
      wrappedValueMapper
    )
  }
}
