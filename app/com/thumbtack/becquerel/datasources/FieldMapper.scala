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

import org.apache.olingo.commons.api.data.Property
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.apache.olingo.commons.api.edm.provider.{CsdlComplexType, CsdlProperty}

trait FieldMapper[Column] extends DefinesComplexTypes {
  /**
    * Name of datasource field.
    */
  val name: String

  val property: CsdlProperty

  val valueMapper: ValueMapper[Column]

  /**
    * Convert a datasource field value to an OData property.
    */
  def apply(fieldValue: Column): Property = {
    new Property(
      property.getType,
      property.getName,
      valueMapper.valueType,
      valueMapper(fieldValue)
    )
  }

  def collectComplexTypes: Map[FullQualifiedName, CsdlComplexType] = {
    valueMapper.collectComplexTypes
  }
}
