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

// scalastyle:off custom.no-println

package com.thumbtack.becquerel.demo

import java.lang.reflect.Constructor
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.UUID
import java.util.concurrent.{TimeUnit, TimeoutException}

import scala.collection.JavaConverters._
import scala.concurrent.duration._

import com.google.cloud.WaitForOption
import com.google.cloud.bigquery._
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.{BlobId, BlobInfo, Storage, StorageOptions}
import com.twitter.util.StorageUnit
import com.typesafe.config.{Config, ConfigFactory}
import resource._

/**
  * Load the Dell DVD Store data files into BQ.
  *
  * @note Requires a writable GCS bucket for staging data files.
  */
object BqDemoLoader {

  def main(args: Array[String]): Unit = {
    val bq: BigQuery = BigQueryOptions.getDefaultInstance.getService
    val gcs: Storage = StorageOptions.getDefaultInstance.getService

    for (bqTable <- bqTables) {
      for (gcsTempDir <- gcsManagedTmpDir(gcs, BqDemoConfig.gcsTempBucket, s"BqDemoLoader-${bqTable.dvdStoreTable.name}")) {
        // Upload all of the table's data files to the temporary directory.
        gcsUploadTableData(gcs, BqDemoConfig.gcsTempBucket, gcsTempDir, bqTable.dvdStoreTable.paths(SharedConfig.dvdStoreDir))

        // Load the data into a new BigQuery table.
        val gcsDataGlobURL = s"gs://${BqDemoConfig.gcsTempBucket}/$gcsTempDir*.csv"
        val bqTableID = TableId.of(BqDemoConfig.projectID, BqDemoConfig.datasetID, s"${BqDemoConfig.tableIDPrefix}${bqTable.dvdStoreTable.name}")
        val jobID = JobId.of(BqDemoConfig.projectID, s"BqDemoLoader-${UUID.randomUUID()}")
        println(s"Copying $gcsDataGlobURL into ${bqTableID.getProject}:${bqTableID.getDataset}.${bqTableID.getTable} " +
          s"with job ${jobID.getProject}:${jobID.getJob}")
        bqLoadTableData(bq, jobID, SharedConfig.timeout, bqTableID, gcsDataGlobURL, bqTable.schema)
      }
    }
  }

  private[demo] def gcsUploadTableData(
    gcs: Storage,
    gcsTempBucket: String,
    gcsTempDir: String,
    csvPaths: Seq[Path]
  ): Unit = {
    // Merge CSVs and change date format from yyyy/mm/dd to yyyy-mm-dd.
    val tempPath = Files.createTempFile("data", ".csv")
    tempPath.toFile.deleteOnExit()
    for (tempOut <- managed(Files.newBufferedWriter(tempPath, StandardOpenOption.CREATE))) {
      for (csvPath <- csvPaths) {
        for (csvIn <- managed(Files.newBufferedReader(csvPath))) {
          for (line <- csvIn.lines().iterator().asScala) {
            tempOut.write(line.replaceAll("""\b(\d{4})/(\d{2})/(\d{2})\b""", """$1-$2-$3"""))
            tempOut.write('\n')
          }
        }
      }
    }

    println(s"Uploading $tempPath to gs://$gcsTempBucket/$gcsTempDir")
    val blob = BlobInfo
      .newBuilder(BlobId.of(
        gcsTempBucket,
        s"$gcsTempDir${tempPath.getFileName}"
      ))
      .setContentType("text/csv")
      .build()
    for {
      csvFileChannel <- managed(FileChannel.open(tempPath, StandardOpenOption.READ))
      gcsWriter <- managed(gcs.writer(blob))
    } {
      val expectedBytes = Files.size(tempPath)
      val actualBytes = csvFileChannel.transferTo(0, expectedBytes, gcsWriter)
      if (expectedBytes != actualBytes) {
        throw new Exception(
          s"Failed to copy $tempPath to ${blob.getBlobId}: " +
            s"expected to upload ${StorageUnit.fromBytes(expectedBytes).toHuman()}, " +
            s"but could only upload ${StorageUnit.fromBytes(actualBytes).toHuman()}!"
        )
      }
    }
  }

  private[demo] def bqLoadTableData(
    bq: BigQuery,
    jobID: JobId,
    timeout: FiniteDuration,
    bqTableID: TableId,
    gcsDataGlobURL: String,
    schema: Schema
  ): Job = {
    val loadJob: Job = bq.create(
      JobInfo.newBuilder(
        LoadJobConfiguration
          .newBuilder(bqTableID, gcsDataGlobURL)
          .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
          .setWriteDisposition(JobInfo.WriteDisposition.WRITE_TRUNCATE)
          .setIgnoreUnknownValues(false)
          .setFormatOptions(CsvOptions
            .newBuilder()
            .setSkipLeadingRows(0)
            .setAllowJaggedRows(false)
            .build()
          )
          .setSchema(schema)
          .build()
      )
        .setJobId(jobID)
        .build()
    )
    bqWaitForJob(loadJob, timeout)
  }

  private[demo] val fieldTypeCtor: Constructor[Field.Type] = {
    val ctor = classOf[Field.Type].getDeclaredConstructor(classOf[LegacySQLTypeName])
    ctor.setAccessible(true)
    ctor
  }

  /**
    * This version of the BQ client library omits support for temporal types when creating tables.
    */
  private[demo] def fieldType(typeName: LegacySQLTypeName): Field.Type = {
    fieldTypeCtor.newInstance(typeName)
  }

  private[demo] val bqTables = Seq[BqDvdStoreTable](
    BqDvdStoreTable(
      dvdStoreTable = DvdStoreTable.customers,
      schema = Schema.of(
        Field
          .newBuilder("customerid", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("firstname", Field.Type.string())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("lastname", Field.Type.string())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("address1", Field.Type.string())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("address2", Field.Type.string())
          .setMode(Field.Mode.NULLABLE)
          .build(),
        Field
          .newBuilder("city", Field.Type.string())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("state", Field.Type.string())
          .setMode(Field.Mode.NULLABLE)
          .build(),
        Field
          .newBuilder("zip", Field.Type.string())
          .setMode(Field.Mode.NULLABLE)
          .build(),
        Field
          .newBuilder("country", Field.Type.string())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("region", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("email", Field.Type.string())
          .setMode(Field.Mode.NULLABLE)
          .build(),
        Field
          .newBuilder("phone", Field.Type.string())
          .setMode(Field.Mode.NULLABLE)
          .build(),
        Field
          .newBuilder("creditcardtype", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("creditcard", Field.Type.string())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("creditcardexpiration", Field.Type.string())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("username", Field.Type.string())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("password", Field.Type.string())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("age", Field.Type.integer())
          .setMode(Field.Mode.NULLABLE)
          .build(),
        Field
          .newBuilder("income", Field.Type.integer())
          .setMode(Field.Mode.NULLABLE)
          .build(),
        Field
          .newBuilder("gender", Field.Type.string())
          .setMode(Field.Mode.NULLABLE)
          .build()
      )
    ),
    BqDvdStoreTable(
      dvdStoreTable = DvdStoreTable.cust_hist,
      schema = Schema.of(
        Field
          .newBuilder("customerid", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("orderid", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("prod_id", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build()
      )
    ),
    BqDvdStoreTable(
      dvdStoreTable = DvdStoreTable.orders,
      schema = Schema.of(
        Field
          .newBuilder("orderid", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("orderdate", fieldType(LegacySQLTypeName.DATE))
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("customerid", Field.Type.integer())
          .setMode(Field.Mode.NULLABLE)
          .build(),
        Field
          .newBuilder("netamount", Field.Type.floatingPoint())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("tax", Field.Type.floatingPoint())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("totalamount", Field.Type.floatingPoint())
          .setMode(Field.Mode.REQUIRED)
          .build()
      )
    ),
    BqDvdStoreTable(
      dvdStoreTable = DvdStoreTable.orderlines,
      schema = Schema.of(
        Field
          .newBuilder("orderlineid", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("orderid", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("prod_id", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("quantity", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("orderdate", fieldType(LegacySQLTypeName.DATE))
          .setMode(Field.Mode.REQUIRED)
          .build()
      )
    ),
    BqDvdStoreTable(
      dvdStoreTable = DvdStoreTable.products,
      schema = Schema.of(
        Field
          .newBuilder("prod_id", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("category", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("title", Field.Type.string())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("actor", Field.Type.string())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("price", Field.Type.floatingPoint())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("special", Field.Type.integer())
          .setMode(Field.Mode.NULLABLE)
          .build(),
        Field
          .newBuilder("common_prod_id", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build()
      )
    ),
    BqDvdStoreTable(
      dvdStoreTable = DvdStoreTable.inventory,
      schema = Schema.of(
        Field
          .newBuilder("prod_id", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("quan_in_stock", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("sales", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build()
      )
    ),
    BqDvdStoreTable(
      dvdStoreTable = DvdStoreTable.categories,
      schema = Schema.of(
        Field
          .newBuilder("category", Field.Type.integer())
          .setMode(Field.Mode.REQUIRED)
          .build(),
        Field
          .newBuilder("categoryname", Field.Type.string())
          .setMode(Field.Mode.REQUIRED)
          .build()
      )
    )
  )

  /**
    * Create a managed remote temporary directory, which is deleted with its contents when it's closed.
    *
    * @return Path relative to the bucket, ending with a slash.
    */
  private[demo] def gcsManagedTmpDir(gcs: Storage, bucket: String, prefix: String): ManagedResource[String] = {
    // Trailing slash is significant.
    val path = s"$prefix${UUID.randomUUID()}/"

    val blob: BlobInfo = BlobInfo.newBuilder(bucket, path).build()

    // Insert a zero-length file as a directory placeholder.
    def open(): String = {
      gcs.create(blob, new Array[Byte](0))
      path
    }

    // Delete everything under the directory's prefix.
    def close(otherPath: String): Unit = {
      assert(path == otherPath)

      val blobs = gcs
        .list(bucket, BlobListOption.prefix(path))
        .iterateAll()
        .asScala
        .map(_.getBlobId)
        .toIndexedSeq

      val result = gcs.delete(blobs.asJava)
      assert(result.asScala.forall(_.booleanValue()), "Failed to delete blobs.")
    }

    makeManagedResource(open())(close)(List.empty)
  }

  /**
    * Wait for a BigQuery job to complete. Will try to cancel it if it runs over our timeout.
    */
  private[demo] def bqWaitForJob(job: Job, timeout: FiniteDuration): Job = {
    try {
      val result = job.waitFor(WaitForOption.timeout(timeout.toMillis, TimeUnit.MILLISECONDS))
      result match {
        case null => throw new Exception(s"Job ${job.getJobId} disappeared!")
        case completedJob => completedJob.getStatus.getError match {
          case null => completedJob
          case error => throw new Exception(s"Job ${job.getJobId} hit an error: $error")
        }
      }
    } catch {
      case e: TimeoutException =>
        // Try to cancel the job.
        val cancelStatus = if (job.cancel()) {
          "was cancelled successfully"
        } else {
          "couldn't be cancelled"
        }
        throw new Exception(s"Timed out waiting for job ${job.getJobId}. It $cancelStatus.", e)
    }
  }

}

object BqDemoConfig {
  private[demo] lazy val bq: Config = SharedConfig.demo
    .withFallback(ConfigFactory.parseMap(Map(
      "bq" -> Map().asJava
    ).asJava))
    .getConfig("bq")
    .withFallback(ConfigFactory.parseMap(Map(
      "tableIDPrefix" -> ""
    ).asJava))
  lazy val projectID: String = bq.getString("projectID")
  lazy val datasetID: String = bq.getString("datasetID")
  lazy val tableIDPrefix: String = bq.getString("tableIDPrefix")
  lazy val gcsTempBucket: String = bq.getString("gcsTempBucket")
}

/**
  * How to map a DVD Store table to a BQ table.
  */
private[demo] case class BqDvdStoreTable(
  dvdStoreTable: DvdStoreTable,
  schema: Schema
)
