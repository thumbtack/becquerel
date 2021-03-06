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

import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.apache.olingo.commons.api.edm.provider.{CsdlComplexType, CsdlEntitySet, CsdlEntityType}

case class TableMapper[Column, Row](
  tableIDParts: Seq[String],
  entitySet: CsdlEntitySet,
  entityType: CsdlEntityType,
  rowMapper: RowMapper[Column, Row],
  idFields: Seq[String]
) extends DefinesComplexTypes {
  def collectComplexTypes: Map[FullQualifiedName, CsdlComplexType] = {
    rowMapper
      .fieldMappers
      .flatMap(_.collectComplexTypes)
      .toMap
  }
}
