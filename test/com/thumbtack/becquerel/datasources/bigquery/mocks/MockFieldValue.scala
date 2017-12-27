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

package com.thumbtack.becquerel.datasources.bigquery.mocks

import java.lang.reflect.Constructor

import com.google.cloud.bigquery.FieldValue

import scala.collection.JavaConverters._

/**
  * [[FieldValue]] doesn't have a public constructor. Fake it.
  */
object MockFieldValue {
  val fieldValueCtor: Constructor[_] = classOf[FieldValue].getDeclaredConstructors.head
  fieldValueCtor.setAccessible(true)

  def apply(
    attribute: FieldValue.Attribute,
    value: Any
  ): FieldValue = {
    val valueRef = value match {
      case seq: Seq[_] => seq.asJava
      case null => null
      case _ => value.toString
    }

    fieldValueCtor.newInstance(attribute, valueRef).asInstanceOf[FieldValue]
  }
}
