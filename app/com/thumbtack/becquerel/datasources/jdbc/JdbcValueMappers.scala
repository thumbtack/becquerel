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

import com.thumbtack.becquerel.datasources.{NullableMapper, PrimitiveMapper, ValueMapper}
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind

/**
  * Just pass the Java object value through and hope OData knows what to do with it.
  */
case class JdbcPrimitiveMapper(
  edmPrimitiveTypeKind: EdmPrimitiveTypeKind
) extends ValueMapper[AnyRef]
  with PrimitiveMapper[AnyRef] {

  override def apply(fieldValue: AnyRef): Any = fieldValue
}

case class JdbcNullableMapper(
  wrapped: ValueMapper[AnyRef]
) extends ValueMapper[AnyRef]
  with NullableMapper[AnyRef] {

  override def isNull(fieldValue: AnyRef): Boolean = fieldValue == null
}
