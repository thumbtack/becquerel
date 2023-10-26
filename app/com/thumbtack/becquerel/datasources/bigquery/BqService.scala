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

import java.io.{ByteArrayInputStream, FileInputStream}
import java.nio.charset.StandardCharsets
import java.time.Instant
import javax.inject.{Inject, Singleton}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.control.NonFatal
import play.api.Logger

import com.google.auth.Credentials
import com.google.auth.oauth2.{ServiceAccountCredentials, ServiceAccountJwtAccessCredentials, UserCredentials}
import com.google.cloud.bigquery.{BigQuery, BigQueryException, DatasetId, FieldValue, QueryRequest, TableDefinition, TableId}
import org.apache.calcite.sql.SqlDialect
import play.api.libs.json.Json
import play.api.{Configuration, Logger}

import com.thumbtack.becquerel.datasources.sql.{SqlExpressionCompiler, SqlService}
import com.thumbtack.becquerel.datasources.{DataSourceMetadata, DataSourceServiceConfig, DataSourceServiceConfigFactory, TableMapper}
import com.thumbtack.becquerel.util.Glob._
import com.thumbtack.becquerel.util.{BecquerelException, Glob}
import com.thumbtack.becquerel.{BecquerelServiceFactory, BecquerelServiceFactoryRegistry, BecquerelServiceManager}

/**
  * OData service backed by BigQuery.
  */
class BqService(
  bqConfig: BqServiceConfig
) extends SqlService[FieldValue, Seq[(TableId, TableDefinition)]] {

  protected override def dssConfig: DataSourceServiceConfig = bqConfig.dssConfig
  protected def bqBuilder: BqBuilder = bqConfig.bqBuilder
  protected def projects: Seq[String] = bqConfig.projects
  protected def datasets: Option[Seq[String]] = bqConfig.datasets
  protected def tables: Option[Seq[String]] = bqConfig.tables
  protected def omitProjectID: Boolean = bqConfig.omitProjectID
  protected def metadataAccountProjectID: Option[String] = bqConfig.metadataAccountProjectID
  protected def metadataAccountCredentials: Option[Credentials] = bqConfig.metadataAccountCredentials
  protected def queryPollInterval: FiniteDuration = bqConfig.queryPollInterval

  protected val tableFilter: TableId => Boolean = tables
    .map { globs =>
      val tableRegex = Glob.compile(globs)
      tableID: TableId => tableRegex.matches(tableID.getTable)
    }
    .getOrElse(_ => true)

  protected override def logger: Logger = Logger(getClass)

  override def displayName: String = s"BigQuery service $name"

  override def describe: scala.collection.Map[String, String] = {
    val section = mutable.LinkedHashMap.empty[String, String]
    section ++= super.describe
    section("Project ID") = metadataAccountProjectID.getOrElse("not defined")
    metadataAccountCredentials match {
      case None =>
        section("Credentials class") = "no credentials file"
      case Some(bqCredentials) =>
        section("Credentials class") = bqCredentials.getClass.getName
        bqCredentials match {
          case serviceAccountCredentials: ServiceAccountCredentials =>
            section("Client ID") = serviceAccountCredentials.getClientId
            section("Client email") = serviceAccountCredentials.getClientEmail
          case userCredentials: UserCredentials =>
            // no email
            section("Client ID") = userCredentials.getClientId
          case jwt: ServiceAccountJwtAccessCredentials =>
            section("Client ID") = jwt.getClientId
            section("Client email") = jwt.getClientEmail
          case _ => // Nothing useful to display.
        }
    }
    section
  }

  /**
    * BigQuery connection for fetching metadata.
    */
  protected val metadataBQ: BigQuery = {
    bqBuilder(metadataAccountProjectID, metadataAccountCredentials)
  }

  /**
    * BigQuery connection for fetching data as a given user.
    */
  protected val dataBQ: BigQuery = metadataBQ // TODO: support OAuth with credentials passthrough.

  /**
    * Restrict datasets based on a whitelist.
    */
  protected def filterDatasets(datasetID: DatasetId): Boolean = {
    datasets match {
      case Some(names: Seq[String]) => names.toSet.contains(datasetID.getDataset)
      case None => true
    }
  }

  /**
    * Get all datasets for a project.
    */
  protected def fetchDatasets(projectId: String): Future[Seq[DatasetId]] = {
    implicit val ec: ExecutionContext = metadataEC
    Future {
      blocking {
        metadataBQ
          .listDatasets(projectId)
          .iterateAll()
          .asScala
          .map(_.getDatasetId)
          .filter(filterDatasets)
          .toIndexedSeq
      }
    } recover {
      case NonFatal(e) =>
        logger.warn(s"Failed to fetch datasets for project $projectId", e)
        fetchErrorsMeter.mark()
        Seq.empty
    }
  }

  /**
    * Get all tables for a dataset.
    */
  protected def fetchTables(datasetId: DatasetId): Future[Seq[TableId]] = {
    implicit val ec: ExecutionContext = metadataEC
    Future {
      blocking {
        metadataBQ
          .listTables(datasetId)
          .iterateAll()
          .asScala
          .map(_.getTableId)
          .filter(tableFilter)
          .toIndexedSeq
      }
    } recover {
      case NonFatal(e) =>
        logger.warn(s"Failed to fetch tables for dataset $datasetId", e)
        fetchErrorsMeter.mark()
        Seq.empty
    }
  }

  /**
    * Get the definition for a table, if we're allowed to see it.
    */
  protected def fetchDefinition(tableId: TableId): Future[Option[(TableId, TableDefinition)]] = {
    implicit val ec: ExecutionContext = metadataEC
    BqService.fetchDefinitionWithSchema(metadataEC, metadataBQ)(tableId) map { tableDefinition =>
      Some(tableId -> tableDefinition)
    } recover {
      case e: BigQueryException if Option(e.getError).map(_.getReason).contains("accessDenied") =>
        // We can discover the names of tables that we don't have permission to view,
        // so this error is normal if the metadata service account has restricted permissions.
        logger.debug(s"Access denied to table $tableId")
        None
      case NonFatal(e) =>
        logger.warn(s"Failed to fetch definition for table $tableId", e)
        fetchErrorsMeter.mark()
        None
    }
  }

  /**
    * Get the definition for every table we have access to.
    */
  override def fetchDefinitions(): Future[Seq[(TableId, TableDefinition)]] = {
    implicit val ec: ExecutionContext = metadataEC

    val datasets: Future[Seq[DatasetId]] = Future
      .traverse(projects)(fetchDatasets)
      .map(_.flatten)

    val tables: Future[Seq[TableId]] = datasets
      .flatMap(Future.traverse(_)(fetchTables))
      .map(_.flatten)

    val definitions: Future[Seq[(TableId, TableDefinition)]] = tables
      .flatMap(Future.traverse(_)(fetchDefinition))
      .map(_.flatten)

    definitions
  }

  protected override def parseDefinitions(
    definitions: Seq[(TableId, TableDefinition)],
    timeFetched: Instant
  ): DataSourceMetadata[FieldValue, Seq[FieldValue]] = {

    val mkMapper: (TableId, TableDefinition) => TableMapper[FieldValue, Seq[FieldValue]] = BqTableMapper(namespace, omitProjectID)

    DataSourceMetadata(
      namespace = namespace,
      defaultContainerName = defaultContainerName,
      tableMappers = definitions.map(mkMapper.tupled),
      timeFetched = timeFetched
    )
  }

  /**
    * @return Calcite SQL dialect for this service.
    */
  protected override val dialect: SqlDialect = BqDialect

  /**
    * Expression compiler for this service.
    */
  protected override val expressionCompiler: SqlExpressionCompiler = BqExpressionCompiler

  /**
    * Executes an OData query against a BQ table.
    *
    * @note Google's BQ client library uses a synchronous HTTP client,
    *       and we have to busy-wait for results, so this will take up a whole thread.
    */
  override def execute(sql: String): Future[TraversableOnce[Seq[FieldValue]]] = {
    implicit val ec: ExecutionContext = queryEC

    Future {
      blocking {
        val queryRequest = QueryRequest
          .newBuilder(sql)
          .setUseLegacySql(false)
          .build()

        val queryResponse = dataBQ.query(queryRequest)

        while (!queryResponse.jobCompleted()) {
          // TODO: rewrite this to use the Play scheduler.
          Thread.sleep(queryPollInterval.toMillis)
        }

        if (queryResponse.hasErrors) {
          val message = "BQ query failed with errors"
          logger.error(s"$message: $queryResponse")
          throw new BecquerelException(message)
        }

        queryResponse
          .getResult
          .iterateAll()
          .asScala
          .map(_.asScala)
      }
    }
  }
}

object BqService {
  /**
    * Get a table definition with a valid schema, or throw exceptions if we can't get it.
    */
  def fetchDefinitionWithSchema(ec: ExecutionContext, bq: BigQuery)(tableId: TableId): Future[TableDefinition] = {
    Future {
      blocking {
        val table = bq.getTable(tableId)
        if (table == null) {
          throw new Exception(s"Couldn't get table $tableId. It may be a view that depends on a missing table.")
        }

        val tableDefinition = table.getDefinition[TableDefinition]
        if (tableDefinition.getSchema == null) {
          throw new Exception(s"Table $tableId is missing a schema. It may be an external table with a missing source.")
        }

        tableDefinition
      }
    }(ec)
  }
}

@Singleton
class BqServiceFactory @Inject() (
  factoryRegistry: BecquerelServiceFactoryRegistry,
  bqConfigFactory: BqServiceConfigFactory
) extends BecquerelServiceFactory {

  factoryRegistry(classOf[BqService].getName) = this
  private def logger: Logger = Logger(getClass)

  override def apply(conf: Configuration, serviceManager: BecquerelServiceManager): BqService = {
    try {
      val service = new BqService(bqConfigFactory(conf))
      logger.info(s"Created BqService $service")
      service
    } catch  {
      case e: Exception => logger.error(s"Caught exception: $e")
        throw e
    }
  }
}

/**
  * @param bqBuilder Constructs BigQuery connections.
  * @param projects BigQuery projects to scan for tables.
  * @param datasets Publish tables only from these datasets.
  * @param tables Publish only tables matching these globs.
  *               TODO: create a matcher that can filter on project, dataset, and table together.
  * @param omitProjectID Omit the project ID from table entity set names to make them shorter.
  * @param metadataAccountProjectID Project ID used to initialize the client for BigQuery metadata.
  *                                 If None, will use default from environment if available.
  * @param metadataAccountCredentials Service account credentials for the client for BigQuery metadata.
  *                                   If None, will use default from environment if available.
  * @param queryPollInterval Check a BQ query job for results this often.
  */
case class BqServiceConfig(
  dssConfig: DataSourceServiceConfig,
  bqBuilder: BqBuilder,
  projects: Seq[String],
  datasets: Option[Seq[String]],
  tables: Option[Seq[String]],
  omitProjectID: Boolean,
  metadataAccountProjectID: Option[String],
  metadataAccountCredentials: Option[Credentials],
  queryPollInterval: FiniteDuration
)

class BqServiceConfigFactory @Inject() (
  dssConfigFactory: DataSourceServiceConfigFactory,
  bqBuilder: BqBuilder
) {
  def apply(conf: Configuration): BqServiceConfig = {
    val (
      metadataAccountProjectID: Option[String],
      metadataAccountCredentials: Option[Credentials]
      ) = (conf.getString("credentialsJSON"), conf.getString("credentials")) match {

      case (Some(credentialsText), _) =>
        // Prefer loading them from a JSON string (provided by an environment variable).
        (
          Some((Json.parse(credentialsText) \ "project_id").as[String]),
          Some(ServiceAccountCredentials.fromStream(
            new ByteArrayInputStream(credentialsText.getBytes(StandardCharsets.UTF_8))))
        )
      case (None, Some(credentialsPath)) =>
        // If there's no JSON, look for a file path.
        (
          Some((Json.parse(new FileInputStream(credentialsPath)) \ "project_id").as[String]),
          Some(ServiceAccountCredentials.fromStream(
            new FileInputStream(credentialsPath)))
        )

      case _ =>
        // If there's no credentials file or text provided, Google's API client will get them from the environment.
        // This is useful on developer machines where you've set up the Google Cloud SDK.
        (None, None)
    }

    val projects: Seq[String] = conf
      .getStringSeq("projects")
      .orElse(metadataAccountProjectID.map(projectID => Seq(projectID)))
      .getOrElse {
        val name = conf.getString("name").get
        throw new BecquerelException(s"No projects provided for BigQuery service $name.")
      }

    BqServiceConfig(
      dssConfig = dssConfigFactory(conf),
      bqBuilder = bqBuilder,
      projects = projects,
      datasets = conf
        .getStringSeq("datasets"),
      tables = conf
        .getStringSeq("tables"),
      // Default to not using the project ID if there's only one project.
      omitProjectID = conf
        .getBoolean("omitProjectID")
        .getOrElse(projects.length == 1),
      metadataAccountProjectID = metadataAccountProjectID,
      metadataAccountCredentials = metadataAccountCredentials,
      queryPollInterval = conf
        .getMilliseconds("queryPollInterval")
        .map(_.millis)
        .getOrElse(100.millis)
    )
  }
}
