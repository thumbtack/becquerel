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

package com.thumbtack.becquerel.datasources.sql

import java.time._
import java.util.GregorianCalendar

import com.thumbtack.becquerel.datasources.{ODataStrings, TableMapper}
import com.thumbtack.becquerel.util.CalciteExtensions._
import org.apache.calcite.sql._
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.olingo.commons.api.data.ValueType
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind
import org.apache.olingo.commons.api.edm.{EdmEnumType, EdmPrimitiveTypeKind, EdmType}
import org.apache.olingo.server.api.uri.UriResourceProperty
import org.apache.olingo.server.api.uri.queryoption.expression._
import org.apache.olingo.server.api.uri.queryoption.search._
import org.apache.olingo.server.api.uri.queryoption.{FilterOption, OrderByOption, SearchOption}
import scala.collection.JavaConverters._

/**
  * Translates OData expressions into SQL.
  */
//noinspection NotImplementedCode
trait SqlExpressionCompiler extends ExpressionVisitor[SqlNode] {

  def compile(expression: Expression): SqlNode = {
    expression.accept(this)
  }

  def compileFilter(filter: Option[FilterOption]): Option[SqlNode] = {
    filter.map(_.getExpression).map(compile)
  }

  def compileOrderBy(orderBy: Option[OrderByOption]): Option[SqlNodeList] = {
    orderBy
      .map(_.getOrders.asScala)
      .map { orders =>
        new SqlNodeList(
          orders.map { item =>
            val expr = compile(item.getExpression)
            if (item.isDescending) {
              SqlStdOperatorTable.DESC(expr)
            } else {
              expr
            }
          }.asJava,
          SqlParserPos.ZERO
        )
    }
  }

  override def visitLambdaReference(variableName: String): SqlNode = ???

  override def visitAlias(aliasName: String): SqlNode = ???

  override def visitLambdaExpression(lambdaFunction: String, lambdaVariable: String, expression: Expression): SqlNode = ???

  override def visitEnum(enumType: EdmEnumType, enumValues: java.util.List[String]): SqlNode = ???

  override def visitMember(member: Member): SqlNode = {
    val idParts = member.getResourcePath.getUriResourceParts.asScala.map {
      case uriResourceProperty: UriResourceProperty =>
        uriResourceProperty.getProperty.getName
      case _ => ???
    }
    new SqlIdentifier(idParts.asJava, SqlParserPos.ZERO)
  }

  /**
    * @note `literal.getText` yields the actual text that represents the literal:
    *       for example, the string `O'Reilly` would have the text `"'O''Reilly'"`.
    *
    * @see http://docs.oasis-open.org/odata/odata/v4.0/odata-v4.0-part2-url-conventions.html#_Toc371341809
    * @see https://docs.oasis-open.org/odata/odata/v4.0/os/abnf/odata-abnf-construction-rules.txt
    */
  override def visitLiteral(literal: Literal): SqlNode = {
    if (literal.getType == null) {
      SqlLiteral.createNull(SqlParserPos.ZERO)
    } else {
      literal.getType.getKind match {

        case EdmTypeKind.PRIMITIVE =>
          EdmPrimitiveTypeKind.valueOfFQN(literal.getType.getFullQualifiedName) match {

            case EdmPrimitiveTypeKind.Boolean =>
              SqlLiteral.createBoolean(
                literal.getText.toBoolean,
                SqlParserPos.ZERO)

            case EdmPrimitiveTypeKind.Byte |
                 EdmPrimitiveTypeKind.SByte |
                 EdmPrimitiveTypeKind.Int16 |
                 EdmPrimitiveTypeKind.Int32 |
                 EdmPrimitiveTypeKind.Int64 |
                 EdmPrimitiveTypeKind.Single |
                 EdmPrimitiveTypeKind.Double |
                 EdmPrimitiveTypeKind.Decimal =>
              SqlLiteral.createExactNumeric(
                literal.getText,
                SqlParserPos.ZERO)

            case EdmPrimitiveTypeKind.String =>
              SqlLiteral.createCharString(
                ODataStrings.parseStringLiteral(literal.getText),
                SqlParserPos.ZERO)

            // TODO: define a new kind of Calcite node for these that uses an API newer than `Calendar`.

            case EdmPrimitiveTypeKind.Date =>
              val date = LocalDate.parse(literal.getText)
              val zonedDateTime = date.atStartOfDay(ZoneId.systemDefault())
              val calendar = GregorianCalendar.from(zonedDateTime)
              SqlLiteral.createDate(calendar, SqlParserPos.ZERO)

            case EdmPrimitiveTypeKind.TimeOfDay =>
              val time = LocalTime.parse(literal.getText)
              val zonedDateTime = time
                .atDate(Instant.EPOCH.atZone(ZoneId.systemDefault()).toLocalDate)
                .atZone(ZoneId.systemDefault())
              val calendar = GregorianCalendar.from(zonedDateTime)
              val precision = 3 // Calcite proper doesn't support sub-millisecond precision.
              SqlLiteral.createTime(calendar, precision, SqlParserPos.ZERO)

            case EdmPrimitiveTypeKind.DateTimeOffset =>
              val zonedDateTime = ZonedDateTime.parse(literal.getText)
              val calendar = GregorianCalendar.from(zonedDateTime)
              val precision = 3 // Calcite proper doesn't support sub-millisecond precision.
              SqlLiteral.createTimestamp(calendar, precision, SqlParserPos.ZERO)

            // TODO: bytes type

            case _ => ???
          }

        case _ => ???
      }
    }
  }

  override def visitUnaryOperator(operator: UnaryOperatorKind, operand: SqlNode): SqlNode = {
    operator match {
      case UnaryOperatorKind.MINUS => SqlStdOperatorTable.UNARY_MINUS(operand)
      case UnaryOperatorKind.NOT => SqlStdOperatorTable.NOT(operand)
      case _ => ???
    }
  }

  /**
    * Is this node a literal null?
    */
  protected def isLiteralNull(sqlNode: SqlNode): Boolean = sqlNode match {
    case literal: SqlLiteral => literal.getTypeName == SqlTypeName.NULL
    case _ => false
  }

  override def visitBinaryOperator(operator: BinaryOperatorKind, left: SqlNode, right: SqlNode): SqlNode = {
    operator match {
      case BinaryOperatorKind.MUL => SqlStdOperatorTable.MULTIPLY(left, right)
      case BinaryOperatorKind.DIV => SqlStdOperatorTable.DIVIDE(left, right)
      case BinaryOperatorKind.MOD => SqlStdOperatorTable.MOD(left, right)
      case BinaryOperatorKind.ADD => SqlStdOperatorTable.PLUS(left, right)
      case BinaryOperatorKind.SUB => SqlStdOperatorTable.MINUS(left, right)
      case BinaryOperatorKind.GT => SqlStdOperatorTable.GREATER_THAN(left, right)
      case BinaryOperatorKind.GE => SqlStdOperatorTable.GREATER_THAN_OR_EQUAL(left, right)
      case BinaryOperatorKind.LT => SqlStdOperatorTable.LESS_THAN(left, right)
      case BinaryOperatorKind.LE => SqlStdOperatorTable.LESS_THAN_OR_EQUAL(left, right)
      case BinaryOperatorKind.AND => SqlStdOperatorTable.AND(left, right)
      case BinaryOperatorKind.OR => SqlStdOperatorTable.OR(left, right)

      // When comparing to a null literal, EQ/NE should use IS/IS NOT.
      case BinaryOperatorKind.EQ =>
        if (isLiteralNull(left)) {
          SqlStdOperatorTable.IS_NULL(right)
        } else if (isLiteralNull(right)) {
          SqlStdOperatorTable.IS_NULL(left)
        } else {
          SqlStdOperatorTable.EQUALS(left, right)
        }
      case BinaryOperatorKind.NE =>
        if (isLiteralNull(left)) {
          SqlStdOperatorTable.IS_NOT_NULL(right)
        } else if (isLiteralNull(right)) {
          SqlStdOperatorTable.IS_NOT_NULL(left)
        } else {
          SqlStdOperatorTable.NOT_EQUALS(left, right)
        }

      // TODO: OData-specific HAS operator for enums
      case BinaryOperatorKind.HAS => ???

      case _ => ???
    }
  }

  override def visitMethodCall(methodCall: MethodKind, parameters: java.util.List[SqlNode]): SqlNode = {
    (methodCall, parameters.asScala) match {
      case (MethodKind.CONTAINS, Seq(haystack, needle)) => dialectContains(haystack, needle)
      case (MethodKind.STARTSWITH, Seq(haystack, prefix)) => dialectStartsWith(haystack, prefix)
      case (MethodKind.ENDSWITH, Seq(haystack, suffix)) => dialectEndsWith(haystack, suffix)
      case (MethodKind.TOLOWER, operands @ Seq(_)) => SqlStdOperatorTable.LOWER(operands: _*)
      case (MethodKind.TOUPPER, operands @ Seq(_)) => SqlStdOperatorTable.UPPER(operands: _*)

      // TODO: many more functions

      case _ => ???
    }
  }

  def dialectContains(haystack: SqlNode, needle: SqlNode): SqlNode

  def dialectContainsIgnoreCase(haystack: SqlNode, needle: SqlNode): SqlNode

  def dialectStartsWith(haystack: SqlNode, prefix: SqlNode): SqlNode

  def dialectEndsWith(haystack: SqlNode, suffix: SqlNode): SqlNode

  override def visitTypeLiteral(edmType: EdmType): SqlNode = {
    val typeName: SqlIdentifier = edmType.getKind match {

      case EdmTypeKind.PRIMITIVE =>
        dialectPrimitiveType(EdmPrimitiveTypeKind.valueOfFQN(edmType.getFullQualifiedName))

      case _ => ???
    }

    new SqlDataTypeSpec(
      typeName,
      0,
      0,
      null,
      null,
      SqlParserPos.ZERO)
  }

  def dialectPrimitiveType(edmPrimitiveTypeKind: EdmPrimitiveTypeKind): SqlIdentifier

  /**
    * Olingo search expressions are different from other expressions,
    * so this doesn't use the [[ExpressionVisitor]] interface.
    *
    * @note Assumes we want to do a case-insensitive substring search in each column,
    *       as this seems the most useful, if not the cheapest.
    *
    * @param tableMapper We need the list of columns to be able to search their text.
    */
  def compileSearch[Column](
    tableMapper: TableMapper[Column, Seq[Column]],
    search: Option[SearchOption]
  ): Option[SqlNode] = {
    search
      .map(_.getSearchExpression)
      .flatMap { expr =>
        // TODO: don't try to handle full text search on struct or array columns yet.
        val columnsAsStrings = tableMapper.rowMapper.fieldMappers
          .filter(_.valueMapper.valueType == ValueType.PRIMITIVE)
          .map { field =>
            val columnID = new SqlIdentifier(field.name, SqlParserPos.ZERO)
            if (field.property.getTypeAsFQNObject == EdmPrimitiveTypeKind.String.getFullQualifiedName) {
              columnID
            } else {
              SqlStdOperatorTable.CAST(
                columnID,
                dialectPrimitiveType(EdmPrimitiveTypeKind.String)
              )
            }
          }

        if (columnsAsStrings.nonEmpty) {
          Some(translateSearchExpression(columnsAsStrings, expr))
        } else {
          None
        }
      }
  }

  /**
    * Translate an OData free-text search expression into SQL by applying it to the given columns.
    *
    * @see http://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part2-url-conventions/odata-v4.0-errata03-os-part2-url-conventions-complete.html#Search_Expression
    */
  protected def translateSearchExpression(
    columnsAsStrings: Seq[SqlNode],
    expr: SearchExpression
  ): SqlNode = {

    assert(columnsAsStrings.nonEmpty)
    expr match {

      case term: SearchTerm =>
        columnsAsStrings
          .map { columnAsString =>
            dialectContainsIgnoreCase(
              columnAsString,
              SqlLiteral.createCharString(term.getSearchTerm, SqlParserPos.ZERO)
            )
          }
          .reduce(SqlStdOperatorTable.OR(_, _))

      case unary: SearchUnary => unary.getOperator match {
        case SearchUnaryOperatorKind.NOT =>
          SqlStdOperatorTable.NOT(
            translateSearchExpression(columnsAsStrings, unary.getOperand)
          )
        case _ => ???
      }

      case binary: SearchBinary => binary.getOperator match {
        case SearchBinaryOperatorKind.AND =>
          SqlStdOperatorTable.AND(
            translateSearchExpression(columnsAsStrings, binary.getLeftOperand),
            translateSearchExpression(columnsAsStrings, binary.getRightOperand)
          )
        case SearchBinaryOperatorKind.OR =>
          SqlStdOperatorTable.OR(
            translateSearchExpression(columnsAsStrings, binary.getLeftOperand),
            translateSearchExpression(columnsAsStrings, binary.getRightOperand)
          )
        case _ => ???
      }

      case _ => ???
    }
  }
}
