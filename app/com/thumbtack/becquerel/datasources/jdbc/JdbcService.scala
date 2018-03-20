/*
 *    Copyright 2017–2018 Thumbtack
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

import java.sql.{DatabaseMetaData, Types}
import java.time.Instant
import javax.inject.{Inject, Singleton}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlDialect.DatabaseProduct
import play.api.db.{DBApi, Database}
import play.api.{Configuration, Logger}

import com.thumbtack.becquerel.datasources.jdbc.JdbcResultSetIterator._
import com.thumbtack.becquerel.datasources.sql.{SqlExpressionCompiler, SqlService}
import com.thumbtack.becquerel.datasources.{DataSourceMetadata, DataSourceServiceConfig, DataSourceServiceConfigFactory}
import com.thumbtack.becquerel.{BecquerelServiceFactory, BecquerelServiceFactoryRegistry, BecquerelServiceManager}

class JdbcService(
  jdbcConfig: JdbcServiceConfig
) extends SqlService[AnyRef, Seq[JdbcTableDefinition]] {

  protected override def dssConfig: DataSourceServiceConfig = jdbcConfig.dssConfig
  protected def db: Database = jdbcConfig.db
  protected def omitCatalogID: Boolean = jdbcConfig.omitCatalogID
  protected def omitSchemaID: Boolean = jdbcConfig.omitSchemaID
  protected def safeTableTypes: Set[String] = jdbcConfig.safeTableTypes

  protected override def logger: Logger = Logger(getClass)

  override def displayName: String = s"JDBC service $name"

  override def describe: scala.collection.Map[String, String] = {
    val section = mutable.LinkedHashMap.empty[String, String]
    section ++= super.describe
    section("URL") = db.url
    section("Calcite dialect") = dialect.getDatabaseProduct.toString
    section
  }

  protected[jdbc] override def fetchDefinitions(): Future[Seq[JdbcTableDefinition]] = {
    implicit val ec: ExecutionContext = metadataEC
    Future {
      blocking {
        db.withConnection { conn =>
          val dbmd: DatabaseMetaData = conn.getMetaData

          val catalogs: Iterator[String] = dbmd.getCatalogs
            .iterator.map { case Seq(catalog: String) => catalog }

          val schemas: Iterator[(String, String)] = catalogs.flatMap { catalog =>
            dbmd.getSchemas(
              catalog,
              null // no schema filter
            )
              .iterator.map { case Seq(schema: String, _) => (catalog, schema) }
          }

          val tables: Iterator[JdbcTableDefinition] = schemas.flatMap { case (catalog, schema) =>
            dbmd.getTables(
              catalog,
              schema,
              null, // no table filter
              null // no table type filter
            )
              .iterator.map { case Seq(_, _, table: String, tableType: String, comment, _*) =>
              JdbcTableDefinition(
                catalog = catalog,
                schema = schema,
                table = table,
                tableType = tableType,
                comment = comment match {
                  case x: String => Some(x)
                  case null => None
                }
              )
            }
          }

          val tableDefinitions: Iterator[JdbcTableDefinition] = tables.map { tableDefinition =>
            val columns = dbmd.getColumns(
              tableDefinition.catalog,
              tableDefinition.schema,
              tableDefinition.table,
              null // no column filter
            )
              .iterator.map { case Seq(_, _, _, name: String, dataType: Integer, typeName: String, size, _, decimalDigits, _, nullable: Integer, comment, _*) =>

              val numericScaleOverride: Option[Int] = {
                // PostgresSQL has some nonstandard behavior around unspecified precision.
                // Its JDBC driver reports SQL standard values for precision and scale,
                // but anyone specifying the shorthand form NUMERIC instead of NUMERIC(…, …)
                // is relying on the PG default behavior of large precision and scale.
                // Fortunately, we can detect when the precision was not set explicitly.
                // See https://www.postgresql.org/docs/10/static/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
                //
                // Olingo doesn't seem to support the "variable" option for the Scale facet:
                // https://issues.apache.org/jira/browse/OLINGO-793
                // If a PG DECIMAL/NUMERIC column has a huge precision and 0 scale,
                // we will fill in a large scale as well instead of the standard 0.
                dialect.getDatabaseProduct match {
                  case DatabaseProduct.POSTGRESQL => dataType.intValue() match {
                    case Types.DECIMAL | Types.NUMERIC => size match {
                      case x: Integer if x > JdbcService.PgScaleOverrideSizeThreshold =>
                        Some(JdbcService.PgMaxScale)
                      case _ => None
                    }
                    case _ => None
                  }
                  case _ => None
                }
              }

              JdbcColumnDefinition(
                name = name,
                dataType = dataType,
                typeName = typeName,
                size = size match {
                  case x: Integer => Some(x)
                  case null => None
                },
                decimalDigits = decimalDigits match {
                  case _ if numericScaleOverride.isDefined => numericScaleOverride
                  case x: Integer => Some(x)
                  case null => None
                },
                nullable = nullable.toInt match {
                  case DatabaseMetaData.columnNoNulls => Some(false)
                  case DatabaseMetaData.columnNullable => Some(true)
                  case DatabaseMetaData.columnNullableUnknown => None
                },
                comment = comment match {
                  case x: String => Some(x)
                  case null => None
                }
              )
            }

            tableDefinition.copy(
              columns = columns.toIndexedSeq
            )
          }

          // Force evaluation of entire iterator before connection is closed.
          tableDefinitions.toIndexedSeq
        }
      }
    }
  }

  override def parseDefinitions(
    definitions: Seq[JdbcTableDefinition],
    timeFetched: Instant
  ): DataSourceMetadata[AnyRef, Seq[AnyRef]] = {
    DataSourceMetadata(
      namespace = namespace,
      defaultContainerName = defaultContainerName,
      tableMappers = definitions
        .filter(tableDefinition => safeTableTypes.contains(tableDefinition.tableType))
        .map(JdbcTableMapper(namespace, omitCatalogID, omitSchemaID)),
      timeFetched = timeFetched
    )
  }

  override val dialect: SqlDialect = {
    db.withConnection { conn => SqlDialect.create(conn.getMetaData) }
  }

  override def expressionCompiler: SqlExpressionCompiler = {
    dialect.getDatabaseProduct match {
      case DatabaseProduct.POSTGRESQL => PgExpressionCompiler
      case _ => PgExpressionCompiler // TODO: support other dialects.
    }
  }

  override def execute(sql: String): Future[TraversableOnce[Seq[AnyRef]]] = {
    implicit val ec: ExecutionContext = queryEC

    Future {
      blocking {
        db.withConnection { conn =>
          // Fetch the whole thing at once so we can close the connection.
          conn.createStatement().executeQuery(sql).toIndexedSeq
        }
      }
    }
  }
}

object JdbcService {

  /**
    * Column size threshold for detecting a default scale.
    */
  val PgScaleOverrideSizeThreshold = 1000

  /**
    * Max scale as of PG 9.x.
    */
  val PgMaxScale = 16383
}

/**
  * @see [[java.sql.DatabaseMetaData#getTables]]
  */
case class JdbcTableDefinition(
  catalog: String,
  schema: String,
  table: String,
  tableType: String,
  comment: Option[String],
  columns: IndexedSeq[JdbcColumnDefinition] = IndexedSeq.empty
)

/**
  * @param dataType See [[java.sql.Types]] and [[java.sql.JDBCType]].
  *
  * @see [[java.sql.DatabaseMetaData#getColumns]]
  */
case class JdbcColumnDefinition(
  name: String,
  dataType: Int,
  typeName: String,
  size: Option[Int],
  decimalDigits: Option[Int],
  nullable: Option[Boolean],
  comment: Option[String]
)

@Singleton
class JdbcServiceFactory @Inject() (
  factoryRegistry: BecquerelServiceFactoryRegistry,
  jdbcConfigFactory: JdbcServiceConfigFactory
) extends BecquerelServiceFactory {

  factoryRegistry(classOf[JdbcService].getName) = this

  override def apply(conf: Configuration, serviceManager: BecquerelServiceManager): JdbcService = {
    new JdbcService(jdbcConfigFactory(conf))
  }
}

case class JdbcServiceConfig(
  dssConfig: DataSourceServiceConfig,
  db: Database,
  omitCatalogID: Boolean,
  omitSchemaID: Boolean,
  safeTableTypes: Set[String]
)

@Singleton
class JdbcServiceConfigFactory @Inject() (
  dssConfigFactory: DataSourceServiceConfigFactory,
  dbApi: DBApi
) {
  def apply(conf: Configuration): JdbcServiceConfig = {
    JdbcServiceConfig(
      dssConfig = dssConfigFactory(conf),
      db = dbApi.database(
        conf
          .getString("playDBName")
          .getOrElse("default")
      ),
      // PG and H2 don't have multi-catalog support.
      omitCatalogID = conf
        .getBoolean("omitCatalogID")
        .getOrElse(false),
      // Some applications only use one schema.
      omitSchemaID = conf
        .getBoolean("omitSchemaID")
        .getOrElse(false),
      safeTableTypes = conf
        .getStringSeq("safeTableTypes")
        .map(_.toSet)
        .getOrElse(Set.empty)
    )
  }
}
