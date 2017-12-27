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

import java.sql.Timestamp
import java.time._

import com.google.cloud.bigquery.FieldValue
import com.thumbtack.becquerel.datasources._
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType
import org.apache.olingo.commons.api.edm.{EdmPrimitiveTypeKind, FullQualifiedName}

import scala.collection.JavaConverters._

object BqBooleanMapper extends ValueMapper[FieldValue] with PrimitiveMapper[FieldValue] {
  override val edmPrimitiveTypeKind: EdmPrimitiveTypeKind = EdmPrimitiveTypeKind.Boolean

  override def apply(fieldValue: FieldValue): Any = {
    fieldValue.getBooleanValue
  }
}

object BqBytesMapper extends ValueMapper[FieldValue] with PrimitiveMapper[FieldValue] {
  override val edmPrimitiveTypeKind: EdmPrimitiveTypeKind = EdmPrimitiveTypeKind.Binary

  override def apply(fieldValue: FieldValue): Any = {
    fieldValue.getBytesValue
  }
}

object BqDateMapper extends ValueMapper[FieldValue] with PrimitiveMapper[FieldValue] {
  override val edmPrimitiveTypeKind: EdmPrimitiveTypeKind = EdmPrimitiveTypeKind.Date

  override def apply(fieldValue: FieldValue): Any = {
    Timestamp.from(
      LocalDate
        .parse(fieldValue.getStringValue)
        .atStartOfDay(ZoneId.systemDefault())
        .toInstant)
  }
}

/**
  * @note OData does not have a datetime type without a timezone,
  *       so this will treat all datetimes as if they were UTC.
  */
object BqDateTimeMapper extends ValueMapper[FieldValue] with PrimitiveMapper[FieldValue] {
  override val edmPrimitiveTypeKind: EdmPrimitiveTypeKind = EdmPrimitiveTypeKind.DateTimeOffset

  override def apply(fieldValue: FieldValue): Any = {
    Timestamp.from(
      LocalDateTime
        .parse(fieldValue.getStringValue)
        .atZone(ZoneOffset.UTC)
        .toInstant)
  }
}

object BqFloatMapper extends ValueMapper[FieldValue] with PrimitiveMapper[FieldValue] {
  override val edmPrimitiveTypeKind: EdmPrimitiveTypeKind = EdmPrimitiveTypeKind.Double

  override def apply(fieldValue: FieldValue): Any = {
    fieldValue.getDoubleValue
  }
}

object BqIntegerMapper extends ValueMapper[FieldValue] with PrimitiveMapper[FieldValue] {
  override val edmPrimitiveTypeKind: EdmPrimitiveTypeKind = EdmPrimitiveTypeKind.Int64

  override def apply(fieldValue: FieldValue): Any = {
    fieldValue.getLongValue
  }
}

object BqStringMapper extends ValueMapper[FieldValue] with PrimitiveMapper[FieldValue] {
  override val edmPrimitiveTypeKind: EdmPrimitiveTypeKind = EdmPrimitiveTypeKind.String

  override def apply(fieldValue: FieldValue): Any = {
    fieldValue.getStringValue
  }
}

object BqTimeMapper extends ValueMapper[FieldValue] with PrimitiveMapper[FieldValue] {
  override val edmPrimitiveTypeKind: EdmPrimitiveTypeKind = EdmPrimitiveTypeKind.TimeOfDay

  override def apply(fieldValue: FieldValue): Any = {
    Timestamp.from(
      LocalTime
        .parse(fieldValue.getStringValue)
        .atDate(Instant.EPOCH.atZone(ZoneId.systemDefault()).toLocalDate)
        .atZone(ZoneId.systemDefault())
        .toInstant)
  }
}

object BqTimestampMapper extends ValueMapper[FieldValue] with PrimitiveMapper[FieldValue] {
  override val edmPrimitiveTypeKind: EdmPrimitiveTypeKind = EdmPrimitiveTypeKind.DateTimeOffset

  override def apply(fieldValue: FieldValue): Any = {
    val epochMicros: Long = fieldValue.getTimestampValue
    val epochSeconds: Long = epochMicros / 1000000
    val nanos: Long = (epochMicros % 1000000) * 1000
    val instant = Instant.ofEpochSecond(epochSeconds, nanos)
    Timestamp.from(instant)
  }
}

case class BqNullableMapper(wrapped: ValueMapper[FieldValue]) extends ValueMapper[FieldValue]
  with NullableMapper[FieldValue] {

  override def isNull(fieldValue: FieldValue): Boolean = {
    fieldValue.isNull
  }
}

case class BqRepeatedMapper(wrapped: ValueMapper[FieldValue]) extends ValueMapper[FieldValue]
  with CollectionMapper[FieldValue] {

  override def asCollection(fieldValue: FieldValue): Seq[FieldValue] = {
    fieldValue
      .getRepeatedValue
      .asScala
  }
}

case class BqComplexMapper(
  typeFQN: FullQualifiedName,
  complexType: CsdlComplexType,
  fieldMappers: Seq[BqFieldMapper]
) extends ValueMapper[FieldValue] with ComplexMapper[FieldValue] {

  override def asRecords(fieldValue: FieldValue): Seq[FieldValue] = {
    fieldValue
      .getRecordValue
      .asScala
  }
}
