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

import java.nio.file.Files

import scala.collection.JavaConverters._

import com.sksamuel.elastic4s.IndexAndType
import com.sksamuel.elastic4s.http.ElasticDsl._
import com.sksamuel.elastic4s.http.HttpClient
import com.sksamuel.elastic4s.indexes.IndexDefinition
import com.sksamuel.elastic4s.mappings.{BasicFieldDefinition, MappingDefinition}
import com.typesafe.config.{Config, ConfigFactory}
import com.univocity.parsers.common.ParsingContext
import com.univocity.parsers.common.processor.RowProcessor
import com.univocity.parsers.csv.{CsvParser, CsvParserSettings}
import org.elasticsearch.client.ResponseException
import resource._

/**
  * Load the Dell DVD Store data files into an ES cluster.
  */
object EsDemoLoader {

  def main(args: Array[String]): Unit = {
    for (esHttpClient <- managed(HttpClient(EsDemoConfig.url))) {
      for (esTable <- esTables) {

        val indexName = s"${EsDemoConfig.indexPrefix}${esTable.dvdStoreTable.name}"

        try {
          esHttpClient.execute {
            deleteIndex(indexName)
          }.await
          println(s"Deleted index $indexName.")
        } catch {
          case e: ResponseException if e.getResponse.getStatusLine.getStatusCode == 404 =>
            // The index doesn't exist so we don't need to delete it.
        }

        esHttpClient.execute {
          createIndex(indexName) mappings esTable.mapping
        }.await
        println(s"Created index $indexName.")

        esTable.dvdStoreTable.paths(SharedConfig.dvdStoreDir).foreach { csvPath =>
          println(s"Copying $csvPath into $indexName.")

          val settings = new CsvParserSettings()
          val rowProcessor = new EsWriterRowProcessor(esTable, indexName, esHttpClient)
          settings.setProcessor(rowProcessor)

          val parser = new CsvParser(settings)
          parser.parse(Files.newBufferedReader(csvPath))
        }
      }
    }
  }

  private[demo] val esDocType = "row"
  private[demo] val stubIndexAndType = IndexAndType("", "")

  private[demo] val esTables = Seq[EsDvdStoreTable](
    EsDvdStoreTable(
      dvdStoreTable = DvdStoreTable.customers,
      mapping = MappingDefinition(
        `type` = esDocType,
        fields = Seq(
          BasicFieldDefinition(
            name = "customerid",
            `type` = "integer",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "firstname",
            `type` = "keyword",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "lastname",
            `type` = "keyword",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "address1",
            `type` = "keyword",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "address2",
            `type` = "long",
            nullable = Some(true)
          ),
          BasicFieldDefinition(
            name = "city",
            `type` = "keyword",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "state",
            `type` = "keyword",
            nullable = Some(true)
          ),
          BasicFieldDefinition(
            name = "zip",
            `type` = "keyword",
            nullable = Some(true)
          ),
          BasicFieldDefinition(
            name = "country",
            `type` = "keyword",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "region",
            `type` = "short",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "email",
            `type` = "keyword",
            nullable = Some(true)
          ),
          BasicFieldDefinition(
            name = "phone",
            `type` = "keyword",
            nullable = Some(true)
          ),
          BasicFieldDefinition(
            name = "creditcardtype",
            `type` = "integer",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "creditcard",
            `type` = "keyword",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "creditcardexpiration",
            `type` = "keyword",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "username",
            `type` = "keyword",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "password",
            `type` = "keyword",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "age",
            `type` = "short",
            nullable = Some(true)
          ),
          BasicFieldDefinition(
            name = "income",
            `type` = "integer",
            nullable = Some(true)
          ),
          BasicFieldDefinition(
            name = "gender",
            `type` = "keyword",
            nullable = Some(true)
          )
        )
      ),
      parseRow = {
        case Array(
          customerid,
          firstname,
          lastname,
          address1,
          address2,
          city,
          state,
          zip,
          country,
          region,
          email,
          phone,
          creditcardtype,
          creditcard,
          creditcardexpiration,
          username,
          password,
          age,
          income,
          gender
        ) =>
          IndexDefinition(
            indexAndType = stubIndexAndType,
            id = Some(customerid.toInt)
          ).fields(
            "customerid" -> customerid.toInt,
            "firstname" -> firstname,
            "lastname" -> lastname,
            "address1" -> address1,
            "address2" -> address2,
            "city" -> city,
            "state" -> state,
            "zip" -> zip,
            "country" -> country,
            "region" -> region.toShort,
            "email" -> email,
            "phone" -> phone,
            "creditcardtype" -> creditcardtype.toInt,
            "creditcard" -> creditcard,
            "creditcardexpiration" -> creditcardexpiration,
            "username" -> username,
            "password" -> password,
            "age" -> Option(age).map(_.toInt).orNull,
            "income" -> Option(income).map(_.toInt).orNull,
            "gender" -> gender
          )
      }
    ),
    EsDvdStoreTable(
      dvdStoreTable = DvdStoreTable.cust_hist,
      mapping = MappingDefinition(
        `type` = esDocType,
        fields = Seq(
          BasicFieldDefinition(
            name = "customerid",
            `type` = "integer",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "orderid",
            `type` = "integer",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "prod_id",
            `type` = "integer",
            nullable = Some(false)
          )
        )
      ),
      parseRow = {
        case Array(
        customerid,
        orderid,
        prod_id
        ) =>
          IndexDefinition(
            indexAndType = stubIndexAndType,
            id = None
          ).fields(
            "customerid" -> customerid.toInt,
            "orderid" -> orderid.toInt,
            "prod_id" -> prod_id.toInt
          )
      }
    ),
    EsDvdStoreTable(
      dvdStoreTable = DvdStoreTable.orders,
      mapping = MappingDefinition(
        `type` = esDocType,
        fields = Seq(
          BasicFieldDefinition(
            name = "orderid",
            `type` = "integer",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "orderdate",
            `type` = "date",
            format = Some("strict_date"),
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "customerid",
            `type` = "integer",
            nullable = Some(true)
          ),
          BasicFieldDefinition(
            name = "netamount",
            `type` = "scaled_float",
            nullable = Some(false),
            scalingFactor = Option(100.0)
          ),
          BasicFieldDefinition(
            name = "tax",
            `type` = "scaled_float",
            nullable = Some(false),
            scalingFactor = Option(100.0)
          ),
          BasicFieldDefinition(
            name = "totalamount",
            `type` = "scaled_float",
            nullable = Some(false),
            scalingFactor = Option(100.0)
          )
        )
      ),
      parseRow = {
        case Array(
        orderid,
        orderdate,
        customerid,
        netamount,
        tax,
        totalamount
        ) =>
          IndexDefinition(
            indexAndType = stubIndexAndType,
            id = Some(orderid.toInt)
          ).fields(
            "orderid" -> orderid.toInt,
            "orderdate" -> java.sql.Date.valueOf(orderdate.replace('/', '-')),
            "customerid" -> Option(customerid).map(_.toInt).orNull,
            "netamount" -> BigDecimal.exact(netamount),
            "tax" -> BigDecimal.exact(tax),
            "totalamount" -> BigDecimal.exact(totalamount)
          )
      }
    ),
    EsDvdStoreTable(
      dvdStoreTable = DvdStoreTable.orderlines,
      mapping = MappingDefinition(
        `type` = esDocType,
        fields = Seq(
          BasicFieldDefinition(
            name = "orderlineid",
            `type` = "short",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "orderid",
            `type` = "integer",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "prod_id",
            `type` = "integer",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "quantity",
            `type` = "short",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "orderdate",
            `type` = "date",
            format = Some("strict_date"),
            nullable = Some(false)
          )
        )
      ),
      parseRow = {
        case Array(
        orderlineid,
        orderid,
        prod_id,
        quantity,
        orderdate
        ) =>
          IndexDefinition(
            indexAndType = stubIndexAndType,
            id = None
          ).fields(
            "orderlineid" -> orderlineid.toShort,
            "orderid" -> orderid.toInt,
            "prod_id" -> prod_id.toInt,
            "quantity" -> quantity.toShort,
            "orderdate" -> java.sql.Date.valueOf(orderdate.replace('/', '-'))
          )
      }
    ),
    EsDvdStoreTable(
      dvdStoreTable = DvdStoreTable.products,
      mapping = MappingDefinition(
        `type` = esDocType,
        fields = Seq(
          BasicFieldDefinition(
            name = "prod_id",
            `type` = "integer",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "category",
            `type` = "short",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "title",
            `type` = "keyword",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "actor",
            `type` = "keyword",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "price",
            `type` = "scaled_float",
            nullable = Some(false),
            scalingFactor = Option(100.0)
          ),
          BasicFieldDefinition(
            name = "special",
            `type` = "short",
            nullable = Some(true)
          ),
          BasicFieldDefinition(
            name = "common_prod_id",
            `type` = "integer",
            nullable = Some(false)
          )
        )
      ),
      parseRow = {
        case Array(
        prod_id,
        category,
        title,
        actor,
        price,
        special,
        common_prod_id
        ) =>
          IndexDefinition(
            indexAndType = stubIndexAndType,
            id = Some(prod_id.toInt)
          ).fields(
            "prod_id" -> prod_id.toInt,
            "category" -> category.toShort,
            "title" -> title,
            "actor" -> actor,
            "price" -> BigDecimal.exact(price),
            "special" -> Option(special).map(_.toShort).orNull,
            "common_prod_id" -> common_prod_id.toInt
          )
      }
    ),
    EsDvdStoreTable(
      dvdStoreTable = DvdStoreTable.inventory,
      mapping = MappingDefinition(
        `type` = esDocType,
        fields = Seq(
          BasicFieldDefinition(
            name = "prod_id",
            `type` = "integer",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "quan_in_stock",
            `type` = "integer",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "sales",
            `type` = "integer",
            nullable = Some(false)
          )
        )
      ),
      parseRow = {
        case Array(
        prod_id,
        quan_in_stock,
        sales
        ) =>
          IndexDefinition(
            indexAndType = stubIndexAndType,
            id = Some(prod_id.toInt)
          ).fields(
            "prod_id" -> prod_id.toInt,
            "quan_in_stock" -> quan_in_stock.toInt,
            "sales" -> sales.toInt
          )
      }
    ),
    EsDvdStoreTable(
      dvdStoreTable = DvdStoreTable.categories,
      mapping = MappingDefinition(
        `type` = esDocType,
        fields = Seq(
          BasicFieldDefinition(
            name = "category",
            `type` = "integer",
            nullable = Some(false)
          ),
          BasicFieldDefinition(
            name = "categoryname",
            `type` = "keyword",
            nullable = Some(false)
          )
        )
      ),
      parseRow = {
        case Array(
        category,
        categoryname
        ) =>
          IndexDefinition(
            indexAndType = stubIndexAndType,
            id = Some(category.toInt)
          ).fields(
            "category" -> category.toInt,
            "categoryname" -> categoryname
          )
      }
    )
  )
}

object EsDemoConfig {
  private[demo] lazy val es: Config = SharedConfig.demo
    .withFallback(ConfigFactory.parseMap(Map(
      "es" -> Map().asJava
    ).asJava))
    .getConfig("es")
    .withFallback(ConfigFactory.parseMap(Map(
      "url" -> "elasticsearch://localhost:9200",
      "indexPrefix" -> ""
    ).asJava))
  lazy val url: String = es.getString("url")
  lazy val indexPrefix: String = es.getString("indexPrefix")
}

/**
  * How to map a DVD Store table to an ES index.
  */
private[demo] case class EsDvdStoreTable(
  dvdStoreTable: DvdStoreTable,
  mapping: MappingDefinition,
  parseRow: Array[String] => IndexDefinition
)

private[demo] class EsWriterRowProcessor(esTable: EsDvdStoreTable, indexName: String, esHttpClient: HttpClient) extends RowProcessor {

  private val batchSize = 1000
  private var batch = Seq.newBuilder[IndexDefinition]
  private var rows = 0

  override def processStarted(context: ParsingContext): Unit = {
    batch.sizeHint(batchSize)
  }

  override def rowProcessed(row: Array[String], context: ParsingContext): Unit = {
    batch += esTable.parseRow(row).copy(indexAndType = IndexAndType(indexName, EsDemoLoader.esDocType))
    rows += 1
    if (rows == batchSize) {
      writeBatch()
    }
  }

  override def processEnded(context: ParsingContext): Unit = {
    if (rows > 0) {
      writeBatch()
    }
  }

  private def writeBatch(): Unit = {
    esHttpClient.execute {
      bulk(batch.result())
    }.await
    println(s"Wrote $rows rows to $indexName.")
    batch.clear()
    batch.sizeHint(batchSize)
    rows = 0
  }
}
