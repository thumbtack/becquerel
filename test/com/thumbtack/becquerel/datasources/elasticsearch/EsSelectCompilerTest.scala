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

package com.thumbtack.becquerel.datasources.elasticsearch

import java.time.Instant

import com.sksamuel.elastic4s.Hit
import com.sksamuel.elastic4s.requests.common.FetchSourceContext
import com.sksamuel.elastic4s.requests.indexes.IndexMappings
import com.thumbtack.becquerel.datasources.{DataSourceMetadata, TableMapper}
import org.apache.olingo.commons.api.edm.{Edm, EdmEntityType}
import org.apache.olingo.commons.api.edmx.EdmxReference
import org.apache.olingo.server.api.OData
import org.apache.olingo.server.core.uri.parser.UriTokenizer.TokenKind
import org.apache.olingo.server.core.uri.parser.{SelectParser, UriTokenizer}

import org.scalatest.FunSuite

import scala.collection.JavaConverters._

class EsSelectCompilerTest extends FunSuite {

  val namespace = "Test"

  val indexMappings: IndexMappings = IndexMappings(
    index = "test__customers",
    mappings = Map(
      "id" -> Map("type" -> "long"),
      "first_name" -> Map("type" -> "keyword"),
      "last_name" -> Map("type" -> "keyword")
    )
  )

  val tableMapper: TableMapper[AnyRef, Hit] = EsTableMapper(namespace)(indexMappings)

  val metadata: DataSourceMetadata[AnyRef, Hit] = DataSourceMetadata(
    namespace = namespace,
    defaultContainerName = "Data",
    tableMappers = Seq(tableMapper),
    timeFetched = Instant.EPOCH
  )

  def checkQuery(selectText: String, expectedFetchContext: Option[FetchSourceContext]): Unit = {
    val tokenizer = new UriTokenizer(selectText)

    val odata: OData = OData.newInstance()
    val edm: Edm = odata.createServiceMetadata(metadata, Seq.empty[EdmxReference].asJava).getEdm

    val referencedType: EdmEntityType = edm.getEntityType(tableMapper.entitySet.getTypeFQN)
    val referencedIsCollection = true

    val selectOption = Option(new SelectParser(edm).parse(
      tokenizer,
      referencedType,
      referencedIsCollection
    ))
    assert(tokenizer.next(TokenKind.EOF))

    val (_, actualFetchContext) = EsSelectCompiler.compile(tableMapper, selectOption)

    assert(actualFetchContext === expectedFetchContext)
  }

  test("select ID field") {
    checkQuery(
      selectText = "id",
      expectedFetchContext = Some(new FetchSourceContext(
        true,
        Set("id"),
        Set()
      ))
    )
  }

  test("select non-ID field") {
    checkQuery(
      selectText = "first_name",
      expectedFetchContext = Some(new FetchSourceContext(
        true,
        Set("first_name"),
        Set()
      ))
    )
  }

  test("select two fields") {
    checkQuery(
      selectText = "id,first_name",
      expectedFetchContext = Some(new FetchSourceContext(
        true,
        Set("id", "first_name"),
        Set()
      ))
    )
  }
}
