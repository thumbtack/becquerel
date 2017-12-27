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

import java.time.Instant

import com.thumbtack.becquerel.util.BecquerelException
import org.apache.olingo.commons.api.edm.provider._
import org.apache.olingo.commons.api.edm.{EdmEntitySet, EdmPrimitiveTypeKind, FullQualifiedName}
import play.api.Logger

import scala.collection.JavaConverters._

/**
  * Describe tables and types to both Olingo and our data sources.
  */
case class DataSourceMetadata[Column, Row](
  defaultEntityContainerFQN: FullQualifiedName,
  defaultEntityContainerInfo: CsdlEntityContainerInfo,
  entityTypes: Map[FullQualifiedName, CsdlEntityType],
  complexTypes: Map[FullQualifiedName, CsdlComplexType],
  entitySets: Map[String, CsdlEntitySet],
  entityContainer: CsdlEntityContainer,
  schemas: java.util.List[CsdlSchema],
  tableMappers: Map[String, TableMapper[Column, Row]],
  timeFetched: Instant
) extends CsdlAbstractEdmProvider {
  private val logger = Logger(getClass)

  override def getSchemas: java.util.List[CsdlSchema] = {
    schemas
  }

  override def getEntityContainer: CsdlEntityContainer = {
    entityContainer
  }

  override def getEntityContainerInfo(entityContainerName: FullQualifiedName): CsdlEntityContainerInfo = {
    if (entityContainerName == null || entityContainerName == defaultEntityContainerFQN) {
      defaultEntityContainerInfo
    } else {
      logger.warn(s"Requested unknown entity container: $entityContainerName")
      null
    }
  }

  override def getEntitySet(entityContainerName: FullQualifiedName, entitySetName: String): CsdlEntitySet = {
    if (entityContainerName == defaultEntityContainerFQN) {
      entitySets.getOrElse(entitySetName, null)
    } else {
      logger.warn(s"Requested unknown entity container: $entityContainerName")
      null
    }
  }

  override def getEntityType(entityTypeName: FullQualifiedName): CsdlEntityType = {
    entityTypes.getOrElse(entityTypeName, {
      logger.warn(s"Requested unknown entity type: $entityTypeName")
      null
    })
  }

  override def getComplexType(complexTypeName: FullQualifiedName): CsdlComplexType = {
    complexTypes.getOrElse(complexTypeName, {
      logger.warn(s"Requested unknown complex type: $complexTypeName")
      null
    })
  }

  /**
    * Map an entity set name to a BQ table ID structure.
    * Intended for data datasources outside this class.
    */
  def getTableMapper(entitySet: EdmEntitySet): TableMapper[Column, Row] = {
    val entityContainerName = entitySet.getEntityContainer.getFullQualifiedName
    if (entityContainerName != defaultEntityContainerFQN) {
      throw new BecquerelException(s"Requested unknown entity container: $entityContainerName")
    }
    val entitySetName = entitySet.getName
    tableMappers.getOrElse(entitySetName,
      throw new BecquerelException(s"Requested unknown entity set: $entitySetName"))
  }
}

object DataSourceMetadata {
  def apply[Column, Row](
    namespace: String,
    defaultContainerName: String,
    tableMappers: Seq[TableMapper[Column, Row]],
    timeFetched: Instant
  ): DataSourceMetadata[Column, Row] = {
    // It's not clear why we'd ever need multiple entity containers.
    val defaultEntityContainerFQN = new FullQualifiedName(namespace, defaultContainerName)

    val defaultEntityContainerInfo = new CsdlEntityContainerInfo()
      .setContainerName(defaultEntityContainerFQN)

    val entityTypes = tableMappers
      .map { tableMapper =>
        tableMapper.entitySet.getTypeFQN -> tableMapper.entityType
      }
      .toMap

    val complexTypes = tableMappers
      .flatMap(_.collectComplexTypes)
      .toMap

    // Assumes that all these entity sets are in our single entity container.
    val entitySets = tableMappers
      .map(_.entitySet)
      .map { entitySet =>
        entitySet.getName -> entitySet
      }
      .toMap

    val entityContainer = new CsdlEntityContainer()
      .setName(defaultEntityContainerFQN.getName)
      .setEntitySets(entitySets.values.toSeq.asJava)

    val schemas = Seq(
      new CsdlSchema()
        .setNamespace(namespace)
        .setEntityTypes(entityTypes.values.toSeq.asJava)
        .setComplexTypes(complexTypes.values.toSeq.asJava)
        .setEntityContainer(entityContainer)
    ).asJava

    val tableMappersByEntitySetName =
      tableMappers
      .map { tableMapper =>
        tableMapper.entitySet.getName -> tableMapper
      }
      .toMap

    DataSourceMetadata(
      defaultEntityContainerFQN = defaultEntityContainerFQN,
      defaultEntityContainerInfo = defaultEntityContainerInfo,
      entityTypes = entityTypes,
      complexTypes = complexTypes,
      entitySets = entitySets,
      entityContainer = entityContainer,
      schemas = schemas,
      tableMappers = tableMappersByEntitySetName,
      timeFetched = timeFetched
    )
  }
}
