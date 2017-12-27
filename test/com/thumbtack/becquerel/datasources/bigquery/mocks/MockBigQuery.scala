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

import java.util

import com.google.cloud.Page
import com.google.cloud.bigquery.BigQuery._
import com.google.cloud.bigquery._

/**
  * Returns static data.
  */
//noinspection NotImplementedCode
class MockBigQuery(
  datasets: Map[String, Seq[DatasetId]],
  tables: Map[DatasetId, Seq[(TableId, TableDefinition.Type)]],
  definitions: Map[TableId, TableDefinition],
  data: PartialFunction[QueryRequest, QueryResponse] = Map.empty
) extends BigQuery {

  override def update(datasetInfo: DatasetInfo, options: DatasetOption*): Dataset = ???

  override def update(tableInfo: TableInfo, options: TableOption*): Table = ???

  override def getTable(datasetId: String, tableId: String, options: TableOption*): Table = ???

  override def getTable(tableId: TableId, options: TableOption*): Table = {
    MockTable(this, tableId, definitions(tableId))
  }

  override def getDataset(datasetId: String, options: DatasetOption*): Dataset = ???

  override def getDataset(datasetId: DatasetId, options: DatasetOption*): Dataset = ???

  override def getJob(jobId: String, options: JobOption*): Job = ???

  override def getJob(jobId: JobId, options: JobOption*): Job = ???

  override def writer(writeChannelConfiguration: WriteChannelConfiguration): TableDataWriteChannel = ???

  override def listJobs(options: JobListOption*): Page[Job] = ???

  override def cancel(jobId: String): Boolean = ???

  override def cancel(jobId: JobId): Boolean = ???

  override def listDatasets(options: DatasetListOption*): Page[Dataset] = ???

  override def listDatasets(projectId: String, options: DatasetListOption*): Page[Dataset] = {
    new MockPage[Dataset](datasets(projectId)
      .map(datasetId => MockDataset(this, datasetId)))
  }

  override def delete(datasetId: String, options: DatasetDeleteOption*): Boolean = ???

  override def delete(datasetId: DatasetId, options: DatasetDeleteOption*): Boolean = ???

  override def delete(datasetId: String, tableId: String): Boolean = ???

  override def delete(tableId: TableId): Boolean = ???

  override def listTableData(datasetId: String, tableId: String, options: TableDataListOption*): Page[util.List[FieldValue]] = ???

  override def listTableData(tableId: TableId, options: TableDataListOption*): Page[util.List[FieldValue]] = ???

  override def listTables(datasetId: String, options: TableListOption*): Page[Table] = ???

  override def listTables(datasetId: DatasetId, options: TableListOption*): Page[Table] = {
    new MockPage[Table](
      tables(datasetId)
        .map { case (tableId, tableType) => MockTable(this, tableId, tableType) }
    )
  }

  override def insertAll(request: InsertAllRequest): InsertAllResponse = ???

  override def create(datasetInfo: DatasetInfo, options: DatasetOption*): Dataset = ???

  override def create(tableInfo: TableInfo, options: TableOption*): Table = ???

  override def create(jobInfo: JobInfo, options: JobOption*): Job = ???

  override def query(request: QueryRequest): QueryResponse = {
    data.apply(request)
  }

  override def getQueryResults(jobId: JobId, options: QueryResultsOption*): QueryResponse = ???

  override def getOptions: BigQueryOptions = null

  override def options(): BigQueryOptions = getOptions
}
