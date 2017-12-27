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

import org.apache.olingo.commons.api.edm._
import org.apache.olingo.commons.api.edm.geo.SRID

/**
  * Emulate a primitive EDM property without having to pull in a CSDL property.
  */
//noinspection NotImplementedCode
class MockEdmPrimitiveProperty(name: String, edmType: EdmType) extends EdmProperty {
  override def isUnicode: Boolean = ???

  override def getScale: Integer = ???

  override def isPrimitive: Boolean = ???

  override def getPrecision: Integer = ???

  override def isNullable: Boolean = ???

  override def getSrid: SRID = ???

  override def getDefaultValue: String = ???

  override def getMimeType: String = ???

  override def getMaxLength: Integer = ???

  override def isCollection: Boolean = ???

  override def getAnnotation(term: EdmTerm, qualifier: String): EdmAnnotation = ???

  override def getAnnotations: java.util.List[EdmAnnotation] = ???

  override def getMapping: EdmMapping = ???

  override def getName: String = name

  override def getType: EdmType = edmType
}
