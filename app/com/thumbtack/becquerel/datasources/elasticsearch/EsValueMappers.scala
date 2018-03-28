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

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind

import com.thumbtack.becquerel.datasources.{NullableMapper, PrimitiveMapper, ValueMapper}

/**
  * Parse some ES types that are serialized as strings or numbers; in most other cases,
  * pass the Java object value through and hope OData knows what to do with it.
  *
  * @see [[org.apache.olingo.commons.core.edm.primitivetype.AbstractPrimitiveType#internalValueToString]]
  */
case class EsPrimitiveMapper(
  edmPrimitiveTypeKind: EdmPrimitiveTypeKind
) extends ValueMapper[AnyRef]
  with PrimitiveMapper[AnyRef] {

  override def apply(fieldValue: AnyRef): Any = {
    // TODO: use date format/_meta info to construct mapper instead of this
    // TODO: test (steal the BQ value mapper tests)
    edmPrimitiveTypeKind match {

      case EdmPrimitiveTypeKind.Int64 => fieldValue match {
        case string: String => string.toLong
        case int: java.lang.Integer => int.toLong
        case long: java.lang.Long => long
        case _ => throw new IllegalArgumentException(s"Unexpected type for long field: ${fieldValue.getClass}")
      }

      case EdmPrimitiveTypeKind.Date => fieldValue match {
        case date: String =>
          try {
            Date.valueOf(LocalDate.from(Instant.ofEpochMilli(date.toLong)))
          } catch {
            case _: NumberFormatException =>
              Date.valueOf(LocalDate.parse(date))
          }
        case epochMillis: java.lang.Integer => Date.valueOf(LocalDate.from(Instant.ofEpochMilli(epochMillis.toLong)))
        case epochMillis: java.lang.Long => Date.valueOf(LocalDate.from(Instant.ofEpochMilli(epochMillis)))
        case _ => throw new IllegalArgumentException(s"Unexpected type for date field: ${fieldValue.getClass}")
      }

      case EdmPrimitiveTypeKind.DateTimeOffset => fieldValue match {
        case datetime: String =>
          try {
            Timestamp.from(Instant.ofEpochMilli(datetime.toLong))
          } catch {
            case _: NumberFormatException =>
              Timestamp.from(Instant.parse(datetime))
          }
        case epochMillis: java.lang.Integer => Timestamp.from(Instant.ofEpochMilli(epochMillis.toLong))
        case epochMillis: java.lang.Long => Timestamp.from(Instant.ofEpochMilli(epochMillis))
        case _ => throw new IllegalArgumentException(s"Unexpected type for datetime field: ${fieldValue.getClass}")
      }

      case EdmPrimitiveTypeKind.Decimal => fieldValue match {
        case decimal: String => BigDecimal.exact(decimal).bigDecimal
        case _ => throw new IllegalArgumentException(s"Unexpected type for decimal field: ${fieldValue.getClass}")
      }

      case _ => fieldValue
    }
  }
}

case class EsNullableMapper(
  wrapped: ValueMapper[AnyRef]
) extends ValueMapper[AnyRef]
  with NullableMapper[AnyRef] {

  override def isNull(fieldValue: AnyRef): Boolean = fieldValue == null
}
