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

package com.thumbtack.becquerel.datasources

import java.net.URI

import org.apache.olingo.commons.api.data.{Entity, Property, ValueType}
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind

trait RowMapper[Column, Row] {

  val fieldMappers: Seq[FieldMapper[Column]]
  val idFields: Seq[String]
  val entitySetName: String

  /**
    * Convert a provider row to an OData entity.
    */
  def apply(baseURI: URI)(row: Row): Entity = {
    val entity = new Entity()

    rowToProperties(row).foreach(entity.addProperty)

    entity.setId(baseURI.resolve(generateID(entity, row)))

    entity
  }

  def withFields(fieldMappers: Seq[FieldMapper[Column]]): RowMapper[Column, Row]

  /**
    * Convert the individual columns in a row to a sequence of OData properties.
    */
  protected def rowToProperties(row: Row): Seq[Property]

  /**
    * Generate a relative URI containing the PK for this entity.
    */
  //noinspection NotImplementedCode
  protected def generateID(entity: Entity, row: Row): URI = {
    // TODO: Assumes the selected set of fields actually contains all the ID fields.
    val keyProperties: Seq[Property] = idFields
      .map(idField => fieldMappers.find(_.name == idField).get.property.getName)
      .map(entity.getProperty)

    val keyPredicatesText = keyProperties
      .map {
        case property if property.isNull => "null"

        case property if property.getValueType == ValueType.PRIMITIVE =>

          EdmPrimitiveTypeKind.valueOfFQN(property.getType) match {
            case EdmPrimitiveTypeKind.Boolean |
                 EdmPrimitiveTypeKind.Byte |
                 EdmPrimitiveTypeKind.SByte |
                 EdmPrimitiveTypeKind.Int16 |
                 EdmPrimitiveTypeKind.Int32 |
                 EdmPrimitiveTypeKind.Int64 |
                 EdmPrimitiveTypeKind.Single |
                 EdmPrimitiveTypeKind.Double =>
              property.getValue.toString

            case EdmPrimitiveTypeKind.String =>
              ODataStrings.unparseStringLiteral(property.getValue.toString)

            case _ => ???
          }

        case _ => ???
      }
      .mkString(",")

    new URI(
      null,
      null,
      s"$entitySetName($keyPredicatesText)",
      null
    )
  }
}
