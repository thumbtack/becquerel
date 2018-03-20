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

package com.thumbtack.becquerel.datasources.bigquery

import java.time.Instant

import com.google.cloud.bigquery._
import com.thumbtack.becquerel.datasources.bigquery.mocks.MockFieldValue
import com.thumbtack.becquerel.datasources.{DataSourceMetadata, TableMapper}

import scala.collection.JavaConverters._

/**
  * Mock BigQuery schema and data shared between several tests.
  */
object SharedData {
  val namespace = "Test"

  val projectId: String = "project"

  val omitProjectID = false

  val datasetId: DatasetId = DatasetId.of(projectId, "test")

  val datasetInfo: DatasetInfo = DatasetInfo
    .newBuilder(datasetId)
    .build()

  val tableId: TableId = TableId.of(datasetId.getProject, datasetId.getDataset, "customers")

  /**
    * Table definition for a customers table with an array field and a struct field.
    */
  val tableDefinition: TableDefinition = StandardTableDefinition.newBuilder()
    .setSchema(Schema.newBuilder()
      .addField(Field.newBuilder("id", Field.Type.integer())
        .setMode(Field.Mode.REQUIRED)
        .build())
      .addField(Field.newBuilder("first_name", Field.Type.string())
        .setMode(Field.Mode.REQUIRED)
        .build())
      .addField(Field.newBuilder("last_name", Field.Type.string())
        .setMode(Field.Mode.REQUIRED)
        .build())
      .addField(Field.newBuilder("phone_number", Field.Type.string())
        .setMode(Field.Mode.REPEATED)
        .build())
      .addField(Field.newBuilder("address", Field.Type.record(
        Seq(
          Field.newBuilder("street", Field.Type.string())
            .setMode(Field.Mode.NULLABLE)
            .build(),
          Field.newBuilder("city", Field.Type.string())
            .setMode(Field.Mode.NULLABLE)
            .build(),
          Field.newBuilder("state", Field.Type.string())
            .setMode(Field.Mode.NULLABLE)
            .build(),
          Field.newBuilder("country", Field.Type.string())
            .setMode(Field.Mode.NULLABLE)
            .build(),
          Field.newBuilder("zip", Field.Type.string())
            .setMode(Field.Mode.NULLABLE)
            .build()
        ).asJava))
        .setMode(Field.Mode.REQUIRED)
        .build())
      .build())
    .build()

  /**
    * Data for the customers table.
    */
  val rows: Seq[Seq[FieldValue]] = Seq(
    Seq(
      MockFieldValue(FieldValue.Attribute.PRIMITIVE, 101), // scalastyle:ignore magic.number
      MockFieldValue(FieldValue.Attribute.PRIMITIVE, "Jessica"),
      MockFieldValue(FieldValue.Attribute.PRIMITIVE, "Atreides"),
      MockFieldValue(FieldValue.Attribute.REPEATED, Seq(
        MockFieldValue(FieldValue.Attribute.PRIMITIVE, "(866) 501-5809"),
        MockFieldValue(FieldValue.Attribute.PRIMITIVE, "(855) 846-2825")
      )),
      MockFieldValue(FieldValue.Attribute.RECORD, Seq(
        MockFieldValue(FieldValue.Attribute.PRIMITIVE, null),
        MockFieldValue(FieldValue.Attribute.PRIMITIVE, "Arrakeen"),
        MockFieldValue(FieldValue.Attribute.PRIMITIVE, null),
        MockFieldValue(FieldValue.Attribute.PRIMITIVE, "Arrakis"),
        MockFieldValue(FieldValue.Attribute.PRIMITIVE, null)
      ))
    )
  )

  val metadata: DataSourceMetadata[FieldValue, Seq[FieldValue]] = {
    val mkMapper: (TableId, TableDefinition) => TableMapper[FieldValue, Seq[FieldValue]] = BqTableMapper(
      namespace,
      omitProjectID
    )

    DataSourceMetadata(
      namespace = SharedData.namespace,
      defaultContainerName = "Data",
      tableMappers = Seq(SharedData.tableId -> SharedData.tableDefinition).map(mkMapper.tupled),
      timeFetched = Instant.EPOCH
    )
  }
}
