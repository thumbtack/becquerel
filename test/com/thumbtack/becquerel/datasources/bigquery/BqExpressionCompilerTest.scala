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

import com.google.cloud.bigquery.FieldValue
import com.thumbtack.becquerel.datasources.TableMapper
import org.apache.calcite.sql.SqlNode
import org.apache.olingo.commons.api.edm.{Edm, EdmEntityType, FullQualifiedName}
import org.apache.olingo.commons.api.edmx.EdmxReference
import org.apache.olingo.server.api.OData
import org.apache.olingo.server.api.uri.queryoption._
import org.apache.olingo.server.core.uri.parser.UriTokenizer.TokenKind
import org.apache.olingo.server.core.uri.parser.{FilterParser, OrderByParser, SearchParser, UriTokenizer}
import org.scalatest.FunSuite

import scala.collection.JavaConverters._

/**
  * Tests both `$$filter` and `$$orderby` system query options.
  */
class BqExpressionCompilerTest extends FunSuite {

  val tableMapper: TableMapper[FieldValue, Seq[FieldValue]] = SharedData.metadata.tableMappers("project__test__customers")

  val crossjoinEntitySetNames: java.util.Collection[String] = Seq.empty[String].asJava

  val aliases: java.util.Map[String, AliasQueryOption] = Map.empty[String, AliasQueryOption].asJava

  /**
    * Parse an OData system query option with minimal options, compile it to SQL, and compare it to the expected translation.
    */
  def checkQuery[T <: SystemQueryOption](
    parse: (Edm, OData, UriTokenizer, EdmEntityType, java.util.Collection[String], java.util.Map[String, AliasQueryOption]) => T,
    compile: Option[T] => Option[SqlNode],
    text: String,
    expectedSQL: String
  ): Unit = {
    val tokenizer = new UriTokenizer(text)

    val odata: OData = OData.newInstance()
    val edm: Edm = odata.createServiceMetadata(SharedData.metadata, Seq.empty[EdmxReference].asJava).getEdm

    val referencedType = edm.getEntityType(
      new FullQualifiedName(SharedData.namespace, "project__test__customers")
    )

    val option = Option(parse(
      edm,
      odata,
      tokenizer,
      referencedType,
      crossjoinEntitySetNames,
      aliases
    ))
    assert(tokenizer.next(TokenKind.EOF))

    val ast = compile(option)
    assert(ast.isDefined)

    val actualSQL = ast.get.toSqlString(BqDialect).getSql

    assert(actualSQL === expectedSQL)
  }

  def checkFilter(text: String, expectedSQL: String): Unit = {
    checkQuery(
      parse = new FilterParser(_, _).parse(_, _, _, _),
      compile = BqExpressionCompiler.compileFilter(_: Option[FilterOption]),
      text = text,
      expectedSQL = expectedSQL
    )
  }

  test("filter: string comparision") {
    checkFilter(
      text = "first_name eq 'Jessica'",
      expectedSQL = "`first_name` = 'Jessica'"
    )
  }

  test("filter: string function") {
    checkFilter(
      text = "startswith(first_name,'Jess')",
      expectedSQL = "STARTS_WITH(`first_name`, 'Jess')"
    )
  }

  test("filter: emulated contains function") {
    checkFilter(
      text = "contains(first_name,'ss')",
      expectedSQL = "`first_name` LIKE '%ss%'"
    )
  }

  // SalesForce generates clauses like this for full-text searches if not using $search.
  test("filter: emulated contains function precedence") {
    checkFilter(
      text = "contains(first_name,'ss') eq true",
      expectedSQL = "(`first_name` LIKE '%ss%') = TRUE"
    )
  }

  test("filter: struct access") {
    checkFilter(
      text = "address/country eq 'Arrakis'",
      expectedSQL = "`address`.`country` = 'Arrakis'"
    )
  }

  test("filter: comparision with null") {
    checkFilter(
      text = "address/zip eq null",
      expectedSQL = "`address`.`zip` IS NULL"
    )
  }

  // TODO: cast function not implemented yet.
  ignore("filter: cast to string") {
    checkFilter(
      text = "cast(id,'Edm.String') eq '1'",
      expectedSQL = "CAST(`id` AS STRING) = '1'"
    )
  }

  def checkOrderBy(text: String, expectedSQL: String): Unit = {
    checkQuery(
      parse = new OrderByParser(_, _).parse(_, _, _, _),
      compile = BqExpressionCompiler.compileOrderBy(_: Option[OrderByOption]),
      text = text,
      expectedSQL = expectedSQL
    )
  }

  // Note that since it's the default, Calcite will parse, but not emit, the ASC postfix operator.
  test("orderby: single column, default direction") {
    checkOrderBy(
      text = "first_name",
      expectedSQL = "`first_name`"
    )
  }

  test("orderby: single column, ascending") {
    checkOrderBy(
      text = "first_name asc",
      expectedSQL = "`first_name`"
    )
  }

  test("orderby: single column, descending") {
    checkOrderBy(
      text = "first_name desc",
      expectedSQL = "`first_name` DESC"
    )
  }

  test("orderby: multi-column") {
    checkOrderBy(
      text = "last_name,first_name",
      expectedSQL = "`last_name`, `first_name`"
    )
  }

  def checkSearch(text: String, expectedSQL: String): Unit = {
    checkQuery(
      parse = (_, _, tokenizer, _, _, _) => new SearchParser().parse(tokenizer),
      compile = BqExpressionCompiler.compileSearch(tableMapper, _: Option[SearchOption]),
      text = text,
      expectedSQL = expectedSQL
    )
  }

  test("search: single term") {
    checkSearch(
      text = "Jess",
      expectedSQL =
        """
          (LOWER(CAST(`id` AS `STRING`)) LIKE '%jess%') OR
          (LOWER(`first_name`) LIKE '%jess%') OR
          (LOWER(`last_name`) LIKE '%jess%')
        """.replaceAll("\\s+", " ").trim
    )
  }

  test("search: implicit binary, explicit unary") {
    checkSearch(
      text = "atreides NOT paul",
      expectedSQL =
        """
          ((LOWER(CAST(`id` AS `STRING`)) LIKE '%atreides%') OR
           (LOWER(`first_name`) LIKE '%atreides%') OR
           (LOWER(`last_name`) LIKE '%atreides%'))

          AND NOT

          ((LOWER(CAST(`id` AS `STRING`)) LIKE '%paul%') OR
           (LOWER(`first_name`) LIKE '%paul%') OR
           (LOWER(`last_name`) LIKE '%paul%'))
        """.replaceAll("\\s+", " ").trim
    )
  }
}
