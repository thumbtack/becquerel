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

import com.thumbtack.becquerel.datasources.sql.SqlSelectCompiler
import org.apache.olingo.commons.api.edm.{Edm, FullQualifiedName}
import org.apache.olingo.commons.api.edmx.EdmxReference
import org.apache.olingo.server.api.OData
import org.apache.olingo.server.core.uri.parser.UriTokenizer.TokenKind
import org.apache.olingo.server.core.uri.parser.{SelectParser, UriTokenizer}
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

class BqSelectCompilerTest extends FunSuite {

  def checkQuery(selectText: String, expectedSQL: String): Unit = {
    val tokenizer = new UriTokenizer(selectText)

    val odata: OData = OData.newInstance()
    val edm: Edm = odata.createServiceMetadata(SharedData.metadata, Seq.empty[EdmxReference].asJava).getEdm

    val referencedType = edm.getEntityType(
      new FullQualifiedName(SharedData.namespace, "project__test__customers")
    )

    val referencedIsCollection = true

    val selectOption = Option(new SelectParser(edm).parse(
      tokenizer,
      referencedType,
      referencedIsCollection
    ))
    assert(tokenizer.next(TokenKind.EOF))

    val tableMapper = SharedData.metadata.tableMappers("project__test__customers")

    val (_, selectAST) = SqlSelectCompiler.compile(tableMapper, selectOption)

    val actualSQL = selectAST.toSqlString(BqDialect).getSql

    assert(actualSQL === expectedSQL)
  }

  test("select ID field") {
    checkQuery(
      selectText = "id",
      expectedSQL = "`id`"
    )
  }

  test("select non-ID field") {
    checkQuery(
      selectText = "first_name",
      expectedSQL = "`first_name`"
    )
  }

  test("select two fields") {
    checkQuery(
      selectText = "id,first_name",
      expectedSQL = "`id`, `first_name`"
    )
  }

  // TODO: not implemented yet.
  ignore("select nested field") {
    checkQuery(
      selectText = "address/city",
      expectedSQL = "`address`.`city`"
    )
  }
}
