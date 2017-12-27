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

import java.net.URI

import com.google.cloud.bigquery._
import com.thumbtack.becquerel.datasources.bigquery.mocks.MockEdmPrimitiveProperty
import org.apache.olingo.commons.api.data.ValueType
import org.apache.olingo.commons.api.edm._
import org.apache.olingo.commons.core.edm.primitivetype.{EdmBoolean, EdmInt64, EdmSByte}
import org.apache.olingo.server.api.uri.queryoption.expression.{BinaryOperatorKind, Expression}
import org.apache.olingo.server.core.uri.queryoption.expression.{BinaryImpl, LiteralImpl, MemberImpl}
import org.apache.olingo.server.core.uri.{UriInfoImpl, UriResourcePrimitivePropertyImpl}
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

class BqTableMapperTest extends FunSuite {

  /**
    * We should generate this complex type as part of mapping the table.
    */
  val expectedAddressTypeFQN = new FullQualifiedName(SharedData.namespace, "project__test__customers__address")

  /**
    * Construct a table mapper from a table definition.
    */
  test("BqTableMapper constructor") {
    val tableMapper = BqTableMapper(SharedData.namespace, omitProjectID = false)(SharedData.tableId, SharedData.tableDefinition)
    assert(tableMapper.rowMapper.fieldMappers.nonEmpty)

    val complexTypes = tableMapper.collectComplexTypes
    assert(complexTypes contains expectedAddressTypeFQN)

    val addressType = complexTypes(expectedAddressTypeFQN)
    assert(addressType.getProperties.asScala.nonEmpty)
  }

  /**
    * Convert a BigQuery row to an OData entity.
    */
  test("BqTableMapper apply") {
    /**
      * Customer data for the Lady Jessica.
      */
    val fieldValues: Seq[FieldValue] = SharedData.rows.head

    val tableMapper = BqTableMapper(SharedData.namespace, omitProjectID = false)(SharedData.tableId, SharedData.tableDefinition)
    val baseURI = new URI("")

    val entity = tableMapper.rowMapper(baseURI)(fieldValues)
    assert(entity.getProperties.asScala.nonEmpty)

    val lastName = entity.getProperty("last_name")
    assert(lastName.getValueType === ValueType.PRIMITIVE)
    assert(lastName.getType === EdmPrimitiveTypeKind.String.getFullQualifiedName.toString)
    assert(lastName.asPrimitive() === "Atreides")

    val phoneNumber = entity.getProperty("phone_number")
    assert(phoneNumber.getValueType === ValueType.COLLECTION_PRIMITIVE)
    assert(phoneNumber.getType === EdmPrimitiveTypeKind.String.getFullQualifiedName.toString)
    assert(phoneNumber.asCollection().asScala.head === "(866) 501-5809")

    val address = entity.getProperty("address")
    assert(address.getValueType === ValueType.COMPLEX)
    assert(address.getType === expectedAddressTypeFQN.toString)

    val addressComplexProperties = address.asComplex().getValue.asScala
    assert(addressComplexProperties.nonEmpty)

    val city = addressComplexProperties(1)
    assert(city.getName === "city")
    assert(city.getValueType === ValueType.PRIMITIVE)
    assert(city.getType === EdmPrimitiveTypeKind.String.getFullQualifiedName.toString)
    assert(city.getValue === "Arrakeen")

    val state = addressComplexProperties(2)
    assert(state.getName === "state")
    assert(state.getValueType === ValueType.PRIMITIVE)
    assert(state.getType === EdmPrimitiveTypeKind.String.getFullQualifiedName.toString)
    assert(state.isNull)
  }

  test("BqTableMapper with FilterCompiler") {
    /**
      * Parsed filter expression equivalent to `$filter=id eq 101`.
      */
    val filterExpr: Expression = new BinaryImpl(
      new MemberImpl(
        new UriInfoImpl()
          .addResourcePart(
            new UriResourcePrimitivePropertyImpl(
              new MockEdmPrimitiveProperty("id", EdmInt64.getInstance()))),
        null),
      BinaryOperatorKind.EQ,
      new LiteralImpl(
        "101",
        EdmSByte.getInstance()),
      EdmBoolean.getInstance())

    val filterSQL = BqExpressionCompiler.compile(filterExpr).toSqlString(BqDialect).getSql
    assert(filterSQL === "`id` = 101")
  }
}
