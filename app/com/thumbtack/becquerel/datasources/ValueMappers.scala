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

import org.apache.olingo.commons.api.data.{ComplexValue, ValueType}
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType
import org.apache.olingo.commons.api.edm.{EdmPrimitiveTypeKind, FullQualifiedName}

import scala.collection.JavaConverters._

trait ValueMapper[T] extends DefinesComplexTypes {
  def typeFQN: FullQualifiedName

  def valueType: ValueType

  def apply(fieldValue: T): Any

  def collectComplexTypes: Map[FullQualifiedName, CsdlComplexType]
}

/**
  * Mixin for mapping non-nullable primitive types.
  */
trait PrimitiveMapper[T] extends ValueMapper[T] {
  val edmPrimitiveTypeKind: EdmPrimitiveTypeKind

  override def typeFQN: FullQualifiedName = edmPrimitiveTypeKind.getFullQualifiedName

  override val valueType: ValueType = ValueType.PRIMITIVE

  override def collectComplexTypes: Map[FullQualifiedName, CsdlComplexType] = Map.empty
}

/**
  * Mixin wrapper for nullable types.
  */
trait NullableMapper[T] extends ValueMapper[T] {
  val wrapped: ValueMapper[T]

  override val typeFQN: FullQualifiedName = wrapped.typeFQN

  override val valueType: ValueType = wrapped.valueType

  override def apply(fieldValue: T): Any = {
    if (isNull(fieldValue)) {
      null
    } else {
      wrapped(fieldValue)
    }
  }

  /**
    * Is this value null?
    */
  def isNull(fieldValue: T): Boolean

  override def collectComplexTypes: Map[FullQualifiedName, CsdlComplexType] = {
    wrapped.collectComplexTypes
  }
}

/**
  * Mixin wrapper for collection types.
  */
trait CollectionMapper[T] extends ValueMapper[T] {
  val wrapped: ValueMapper[T]

  override def typeFQN: FullQualifiedName = wrapped.typeFQN

  override val valueType: ValueType = wrapped.valueType match {
    case ValueType.COMPLEX => ValueType.COLLECTION_COMPLEX
    case ValueType.ENTITY => ValueType.COLLECTION_ENTITY
    case ValueType.ENUM => ValueType.COLLECTION_ENUM
    case ValueType.GEOSPATIAL => ValueType.COLLECTION_GEOSPATIAL
    case ValueType.PRIMITIVE => ValueType.COLLECTION_PRIMITIVE

    case other => throw new NotImplementedError(
      s"Can't wrap CSDL value type $other in a collection")
  }

  override def apply(fieldValue: T): Any = {
    asCollection(fieldValue)
      .map(wrapped(_))
      .asJava
  }

  /**
    * Get the individual elements of a collection.
    */
  def asCollection(fieldValue: T): Seq[T]

  override def collectComplexTypes: Map[FullQualifiedName, CsdlComplexType] = {
    wrapped.collectComplexTypes
  }
}

/**
  * Mixin for complex types.
  */
trait ComplexMapper[T] extends ValueMapper[T] {
  val typeFQN: FullQualifiedName

  val complexType: CsdlComplexType

  val fieldMappers: Seq[FieldMapper[T]]

  override def valueType: ValueType = ValueType.COMPLEX

  override def apply(fieldValue: T): Any = {
    val complexValue = new ComplexValue()

    (fieldMappers, asRecords(fieldValue))
      .zipped
      .map(_(_))
      .foreach(complexValue.getValue.add)

    complexValue
  }

  /**
    * Retrieve the records from a complex type, in the same order as [[fieldMappers]].
    */
  def asRecords(fieldValue: T): Seq[T]

  override def collectComplexTypes: Map[FullQualifiedName, CsdlComplexType] = {
    Map((typeFQN, complexType)) ++ fieldMappers.flatMap(_.collectComplexTypes)
  }
}
