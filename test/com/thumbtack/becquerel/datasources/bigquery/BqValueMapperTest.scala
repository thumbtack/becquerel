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

import com.google.cloud.bigquery.FieldValue
import com.thumbtack.becquerel.datasources.bigquery.mocks.MockFieldValue
import org.apache.olingo.commons.api.edm.EdmPrimitiveType
import org.apache.olingo.commons.core.edm.primitivetype.{EdmDate, EdmDateTimeOffset, EdmTimeOfDay}
import org.scalatest.FunSuite

/**
  * Test that BQ timestamps and civil time fields can be converted to OData equivalents.
  */
class BqValueMapperTest extends FunSuite {

  /**
    * Call `edmPrimitiveType.valueToString(value, …)` with default facet values.
    */
  def valueToString(
    edmPrimitiveType: EdmPrimitiveType,
    precision: Option[Int],
    value: Any
  ): String = {
    val isNullable: java.lang.Boolean = null
    val maxLength: java.lang.Integer = null
    val scale: java.lang.Integer = null
    val isUnicode: java.lang.Boolean = null

    edmPrimitiveType.valueToString(
      value,
      isNullable,
      maxLength,
      precision.map(Integer.valueOf).orNull,
      scale,
      isUnicode)
  }

  val bqDatePrecision = None
  val bqTimePrecision = Some(6)

  test("BqDateMapper") {
    val value = BqDateMapper(
      MockFieldValue(FieldValue.Attribute.PRIMITIVE, "2017-02-22"))
    val actual = valueToString(
      EdmDate.getInstance(),
      bqDatePrecision,
      value)
    assert(actual === "2017-02-22")
  }

  test("BqDateTimeMapper") {
    val value = BqDateTimeMapper(
      MockFieldValue(FieldValue.Attribute.PRIMITIVE, "2017-02-22T00:21:05.955102"))
    val actual = valueToString(
      EdmDateTimeOffset.getInstance(),
      bqTimePrecision,
      value)
    assert(actual === "2017-02-22T00:21:05.955102Z")
  }

  test("BqTimeMapper") {
    val value = BqTimeMapper(
      MockFieldValue(FieldValue.Attribute.PRIMITIVE, "00:21:05.955102"))
    val actual = valueToString(
      EdmTimeOfDay.getInstance(),
      bqTimePrecision,
      value)
    assert(actual === "00:21:05.955102")
  }

  test("BqTimestampMapper") {
    val value = BqTimestampMapper(
      MockFieldValue(FieldValue.Attribute.PRIMITIVE, "1.487722865955102E9"))
    val actual = valueToString(
      EdmDateTimeOffset.getInstance(),
      bqTimePrecision,
      value)
    assert(actual === "2017-02-22T00:21:05.955102Z")
  }
}
