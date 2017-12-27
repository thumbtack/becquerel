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

import com.thumbtack.becquerel.datasources.{FieldMapper, ODataStrings, ValueMapper}
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.provider.CsdlProperty

case class EsFieldMapper(
  name: String,
  property: CsdlProperty,
  valueMapper: ValueMapper[AnyRef]
) extends FieldMapper[AnyRef]

object EsFieldMapper {
  def apply(
    namespace: String,
    parentTypeName: String
  )(
    name: String,
    field: Map[String, Any]
  ): EsFieldMapper = {

    val odataName = ODataStrings.sanitizeName(name)

    // TODO: complex types, range types, geo types
    val edmPrimitiveTypeKind = field("type") match {
      case "text" | "keyword" => EdmPrimitiveTypeKind.String
      case "long" => EdmPrimitiveTypeKind.Int64
      case "integer" => EdmPrimitiveTypeKind.Int32
      case "short" => EdmPrimitiveTypeKind.Int16
      case "byte" => EdmPrimitiveTypeKind.SByte
      case "double" => EdmPrimitiveTypeKind.Double
      case "float" => EdmPrimitiveTypeKind.Single
      case "boolean" => EdmPrimitiveTypeKind.Boolean
      case "scaled_float" => EdmPrimitiveTypeKind.Decimal

      case "date" =>
        // By default, ES dates are datetimes with optional times and timezones:
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/date.html
        // How we map these will depend on the date format field.
        // TODO: should prefer _meta edm_type if available.
        // TODO: pass format to the ValueMapper instead of guessing based on EDM type.
        val format = field
          .get("format")
          .map(_
            .asInstanceOf[String]
            .split("""\|\|""")
            .head
          )
          .getOrElse("strict_date_optional_time")
        format match {
          case "strict_date" => EdmPrimitiveTypeKind.Date
          case "strict_date_time" => EdmPrimitiveTypeKind.DateTimeOffset
          case _ => ???
        }

      case "binary" =>
        // TODO: can support these but the ValueMapper needs to do base64 decoding.
        ???

      case _ => ???
    }

    val scale: Option[Int] = field("type") match {
      case "scaled_float" =>
        // Elasticsearch's scaled float type is backed by a long and a scaling factor.
        // It's the closest thing we have to a decimal type.
        // https://www.elastic.co/guide/en/elasticsearch/reference/current/number.html#scaled-float-params
        field("scaling_factor") match {
          case scalingFactor: Int if scalingFactor % 10 == 0 => Some(Math.log10(scalingFactor).toInt)
          case scalingFactor: Double if scalingFactor % 10 == 0 => Some(Math.log10(scalingFactor).toInt)
          case _ => ???
        }
      case _ => None
    }

    val precision: Option[Int] = field("type") match {
      case "scaled_float" =>
        Some(Math.log10(Long.MaxValue).toInt)
      case "date" =>
        // Elasticsearch supports millisecond time resolution.
        Some(3)
      case _ => None
    }

    // TODO: All ES fields are both nullable and can be arrays (there's no array type).
    // For now, we'll assume that all fields are nullable but none of them are arrays.
    // Array fields that are actually meant to be array fields should be marked in _meta;
    // the same for fields that should never be null.
    val valueMapper = EsNullableMapper(EsPrimitiveMapper(edmPrimitiveTypeKind))

    val property = new CsdlProperty()
      .setName(odataName)
      .setCollection(false)
      .setNullable(true)
      .setType(valueMapper.typeFQN)
      .setPrecision(precision.map(Integer.valueOf).orNull)
      .setScale(scale.map(Integer.valueOf).orNull)

    EsFieldMapper(
      name,
      property,
      valueMapper
    )
  }

  /**
    * Dedicated mapper for the ES _id field.
    */
  val idMapper: EsFieldMapper = {

    val name = "_id"
    val odataName = ODataStrings.sanitizeName(name)
    val valueMapper = EsPrimitiveMapper(EdmPrimitiveTypeKind.String)

    val property = new CsdlProperty()
      .setName(odataName)
      .setCollection(false)
      .setNullable(false)
      .setType(valueMapper.typeFQN)

    EsFieldMapper(
      name,
      property,
      valueMapper
    )
  }
}
