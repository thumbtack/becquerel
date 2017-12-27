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

package com.thumbtack.becquerel.demo

import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import resource._

/**
  * Table from the Dell DVD Store data set.
  *
  * @see https://linux.dell.com/dvdstore/
  */
private[demo] case class DvdStoreTable(
  name: String,
  dataFilesGlob: Option[String] = None,
  dataResource: Option[String] = None
) {
  /**
    * @param dvdStoreDir Path to the unpacked Dell DVD Store data directory.
    * @return All CSV data file paths for this table.
    */
  def paths(dvdStoreDir: String): Seq[Path] = {
    dataFilesGlob
      .map(globFiles(dvdStoreDir))
      .getOrElse(Seq.empty) ++
    dataResource
      .map(extractResource)
      .toSeq
  }

  /**
    * @return All file paths under a root that match a glob.
    */
  def globFiles(root: String)(glob: String): Seq[Path] = {
    val builder = Seq.newBuilder[Path]
    val rootPath = Paths.get(root)
    val matcher = rootPath.getFileSystem.getPathMatcher(s"glob:$glob")
    Files.walkFileTree(rootPath, new SimpleFileVisitor[Path] {
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        if (matcher.matches(file)) {
          builder += file
        }
        super.visitFile(file, attrs)
      }
    })
    builder.result()
  }

  /**
    * @param resource A resource name relative to this class.
    * @return A temporary file path containing the contents of that resource.
    *         Will be deleted on normal JVM exit.
    */
  def extractResource(resource: String): Path = {
    val (prefix, suffix) = resource.split("/").last.split("\\.", 2) match {
      case Array(filename, ext) => (filename, s".$ext")
      case Array(filename) => (filename, "")
    }
    val tempPath = Files.createTempFile(prefix, suffix)
    tempPath.toFile.deleteOnExit()

    for (stream <- managed(getClass.getResourceAsStream(resource))) {
      Files.copy(stream, tempPath, StandardCopyOption.REPLACE_EXISTING)
    }

    tempPath
  }
}

/**
  * List of the data sources for the DVD Store tables.
  */
private[demo] object DvdStoreTable {

  val customers = DvdStoreTable(
    name = "customers",
    dataFilesGlob = Some("**/data_files/cust/*.csv")
  )

  val cust_hist = DvdStoreTable(
    name = "cust_hist",
    dataFilesGlob = Some("**/data_files/orders/*_cust_hist.csv")
  )

  val orders = DvdStoreTable(
    name = "orders",
    dataFilesGlob = Some("**/data_files/orders/*_orders.csv")
  )

  val orderlines = DvdStoreTable(
    name = "orderlines",
    dataFilesGlob = Some("**/data_files/orders/*_orderlines.csv")
  )

  val products = DvdStoreTable(
    name = "products",
    dataFilesGlob = Some("**/data_files/prod/prod.csv")
  )

  val inventory = DvdStoreTable(
    name = "inventory",
    dataFilesGlob = Some("**/data_files/prod/inv.csv")
  )

  val categories = DvdStoreTable(
    name = "categories",
    dataFilesGlob = Some("**/data_files/categories.csv")
  )
}
