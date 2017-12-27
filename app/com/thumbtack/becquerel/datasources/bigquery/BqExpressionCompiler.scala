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

import com.thumbtack.becquerel.datasources.sql.{SqlExpressionCompiler, SqlFunctions}
import com.thumbtack.becquerel.util.CalciteExtensions._
import org.apache.calcite.sql._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind

/**
  * Translate a subset of the OData expression language to a Calcite SQL AST.
  *
  * @see https://olingo.apache.org/doc/odata4/tutorials/sqo_f/tutorial_sqo_f.html
  */
//noinspection NotImplementedCode
object BqExpressionCompiler extends SqlExpressionCompiler {
  /**
    * BigQuery Standard SQL doesn't have CONTAINS() so we rewrite it to a LIKE operator.
    */
  override def dialectContains(haystack: SqlNode, needle: SqlNode): SqlNode = {
    needle match {
      case stringLiteral: SqlCharStringLiteral =>
        val pattern = SqlLiteral.createCharString(
          s"%${BqStrings.escapeForLike(stringLiteral.getNlsString.getValue)}%",
          SqlParserPos.ZERO
        )

        SqlFunctions.FORCE_PARENS(
          SqlStdOperatorTable.LIKE(haystack, pattern)
        )

      case _ => ???
    }
  }

  /**
    * BigQuery Standard SQL doesn't have ILIKE.
    * Lower-case the haystack and the needle pattern before comparing.
    */
  def dialectContainsIgnoreCase(haystack: SqlNode, needle: SqlNode): SqlNode = {
    val needleLowerCase = needle match {
      case stringLiteral: SqlCharStringLiteral =>
        SqlLiteral.createCharString(
          stringLiteral.getNlsString.getValue.toLowerCase,
          SqlParserPos.ZERO
        )

      case _ => ???
    }

    dialectContains(
      SqlStdOperatorTable.LOWER(haystack),
      needleLowerCase
    )
  }

  override def dialectStartsWith(haystack: SqlNode, prefix: SqlNode): SqlNode = {
    BqFunctions.STARTS_WITH(haystack, prefix)
  }

  override def dialectEndsWith(haystack: SqlNode, suffix: SqlNode): SqlNode = {
    BqFunctions.ENDS_WITH(haystack, suffix)
  }

  override def dialectPrimitiveType(edmPrimitiveTypeKind: EdmPrimitiveTypeKind): SqlIdentifier = {
    edmPrimitiveTypeKind match {

      case EdmPrimitiveTypeKind.Boolean =>
        new SqlIdentifier(
          "BOOLEAN",
          SqlParserPos.ZERO)

      case EdmPrimitiveTypeKind.Byte |
           EdmPrimitiveTypeKind.SByte |
           EdmPrimitiveTypeKind.Int16 |
           EdmPrimitiveTypeKind.Int32 |
           EdmPrimitiveTypeKind.Int64 =>
        new SqlIdentifier(
          "INT64",
          SqlParserPos.ZERO
        )

      case EdmPrimitiveTypeKind.Single |
           EdmPrimitiveTypeKind.Double =>
        new SqlIdentifier(
          "FLOAT64",
          SqlParserPos.ZERO
        )

      case EdmPrimitiveTypeKind.String =>
        new SqlIdentifier(
          "STRING",
          SqlParserPos.ZERO
        )

      // TODO: date types
      // TODO: bytes type

      case _ => ???
    }
  }
}
