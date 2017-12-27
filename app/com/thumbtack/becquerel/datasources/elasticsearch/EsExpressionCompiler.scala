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

import java.time.{LocalDate, LocalTime, ZonedDateTime}

import com.sksamuel.elastic4s.searches.queries._
import com.sksamuel.elastic4s.searches.queries.term.TermQueryDefinition
import com.sksamuel.elastic4s.searches.sort.{FieldSortDefinition, SortDefinition}

import com.thumbtack.becquerel.datasources.ODataStrings
import org.apache.olingo.commons.api.edm.constants.EdmTypeKind
import org.apache.olingo.commons.api.edm.{EdmEnumType, EdmPrimitiveTypeKind, EdmType}
import org.apache.olingo.server.api.uri.UriResourceProperty
import org.apache.olingo.server.api.uri.queryoption.expression._
import org.apache.olingo.server.api.uri.queryoption.search._
import org.apache.olingo.server.api.uri.queryoption.{FilterOption, OrderByOption, SearchOption}
import org.elasticsearch.search.sort.SortOrder
import scala.collection.JavaConverters._

//noinspection NotImplementedCode
object EsExpressionCompiler extends ExpressionVisitor[EsAstNode] {

  def compile(expression: Expression): EsAstNode = {
    expression.accept(this)
  }

  def compileFilter(filter: Option[FilterOption]): Option[QueryDefinition] = {
    filter
      .map(_.getExpression)
      .map(compile)
      .map(nodeToQueryDef)
  }

  /**
    * The range query builder only takes longs, doubles, and strings.
    *
    * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html
    * @see com.sksamuel.elastic4s.http.search.queries.term.RangeQueryBodyFn.apply
    */
  protected def asRangeParam(value: Any): Option[Any] = {
    value match {
      case x: Long => Some(x)
      case x: Double => Some(x)
      case x => Some(x.toString)
    }
  }

  def nodeToQueryDef(node: EsAstNode): QueryDefinition = {
    node match {

      case EsEquals(EsField(name), EsLiteral(value)) => TermQueryDefinition(name, value)
      case EsEquals(EsLiteral(value), EsField(name)) => TermQueryDefinition(name, value)

      case EsGreaterThan(EsField(name), EsLiteral(value)) => RangeQueryDefinition(name, gt = asRangeParam(value))
      case EsGreaterThan(EsLiteral(value), EsField(name)) => RangeQueryDefinition(name, lte = asRangeParam(value))

      case EsGreaterThanOrEqual(EsField(name), EsLiteral(value)) => RangeQueryDefinition(name, gte = asRangeParam(value))
      case EsGreaterThanOrEqual(EsLiteral(value), EsField(name)) => RangeQueryDefinition(name, lt = asRangeParam(value))

      case EsLessThan(EsField(name), EsLiteral(value)) => RangeQueryDefinition(name, lt = asRangeParam(value))
      case EsLessThan(EsLiteral(value), EsField(name)) => RangeQueryDefinition(name, gte = asRangeParam(value))

      case EsLessThanOrEqual(EsField(name), EsLiteral(value)) => RangeQueryDefinition(name, lte = asRangeParam(value))
      case EsLessThanOrEqual(EsLiteral(value), EsField(name)) => RangeQueryDefinition(name, gt = asRangeParam(value))

      case EsNot(operand) => BoolQueryDefinition(not = Seq(operand).map(nodeToQueryDef))
      case EsAnd(left, right) => BoolQueryDefinition(must = Seq(left, right).map(nodeToQueryDef))
      case EsOr(left, right) => BoolQueryDefinition(should = Seq(left, right).map(nodeToQueryDef))

      case EsStartsWith(EsField(haystack), EsLiteral(prefix)) => PrefixQueryDefinition(haystack, prefix)

      // Use wildcard queries to implement contains() and endswith().
      // Expect these to be much slower than startswith().
      // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html
      case EsContains(EsField(haystack), EsLiteral(needle: String)) => WildcardQueryDefinition(
        haystack,
        s"*${escapeLuceneWildcards(needle)}*"
      )
      case EsEndsWith(EsField(haystack), EsLiteral(suffix: String)) => WildcardQueryDefinition(
        haystack,
        s"*${escapeLuceneWildcards(suffix)}"
      )

      case _ => ???
    }
  }

  /**
    * Escape wildcard characters in a string.
    * @see https://lucene.apache.org/core/4_0_0/core/org/apache/lucene/search/WildcardQuery.html#WILDCARD_ESCAPE
    */
  def escapeLuceneWildcards(s: String): String = {
    s.replaceAll("[*?]", "\\\\$0")
  }

  def compileOrderBy(orderBy: Option[OrderByOption]): Seq[SortDefinition] = {
    orderBy
      .map(_.getOrders.asScala)
      .toSeq.flatMap { orders =>
        orders.map { item =>
          compile(item.getExpression) match {
            case EsField(name) =>
              FieldSortDefinition(
                name,
                order = if (item.isDescending) SortOrder.DESC else SortOrder.ASC
              )
            case _ =>
              // TODO: otherwise, we may have to translate it to a sort script or just refuse to do it.
              ???
          }
        }
      }
  }

  override def visitAlias(aliasName: String): EsAstNode = ???

  override def visitLambdaExpression(lambdaFunction: String, lambdaVariable: String, expression: Expression): EsAstNode = ???

  override def visitLambdaReference(variableName: String): EsAstNode = ???

  override def visitEnum(`type`: EdmEnumType, enumValues: java.util.List[String]): EsAstNode = ???

  override def visitTypeLiteral(`type`: EdmType): EsAstNode = ???

  override def visitMember(member: Member): EsAstNode = {
    val idParts = member.getResourcePath.getUriResourceParts.asScala.map {
      case uriResourceProperty: UriResourceProperty =>
        uriResourceProperty.getProperty.getName
      case _ => ???
    }
    assert(idParts.length == 1)
    // TODO: support nested queries where idParts may have more than one part
    // https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-nested-query.html
    EsField(idParts.head)
  }

  /**
    * @note `literal.getText` yields the actual text that represents the literal:
    *       for example, the string `O'Reilly` would have the text `"'O''Reilly'"`.
    *
    * @see http://docs.oasis-open.org/odata/odata/v4.0/odata-v4.0-part2-url-conventions.html#_Toc371341809
    * @see https://docs.oasis-open.org/odata/odata/v4.0/os/abnf/odata-abnf-construction-rules.txt
    */
  override def visitLiteral(literal: Literal): EsAstNode = {
    if (literal.getType == null) {
      EsLiteral(null)
    } else {
      literal.getType.getKind match {

        case EdmTypeKind.PRIMITIVE =>
          val text = literal.getText

          EdmPrimitiveTypeKind.valueOfFQN(literal.getType.getFullQualifiedName) match {

            // This assumes that OData numeric literals are totally compatible with Java's.
            case EdmPrimitiveTypeKind.Boolean => EsLiteral(text.toBoolean)
            case EdmPrimitiveTypeKind.Byte => EsLiteral(text.toShort)
            case EdmPrimitiveTypeKind.SByte => EsLiteral(text.toByte)
            case EdmPrimitiveTypeKind.Int16 => EsLiteral(text.toShort)
            case EdmPrimitiveTypeKind.Int32 => EsLiteral(text.toInt)
            case EdmPrimitiveTypeKind.Int64 => EsLiteral(text.toLong)
            case EdmPrimitiveTypeKind.Single => EsLiteral(text.toFloat)
            case EdmPrimitiveTypeKind.Double => EsLiteral(text.toDouble)
            case EdmPrimitiveTypeKind.Decimal => EsLiteral(BigDecimal(text))

            case EdmPrimitiveTypeKind.String => EsLiteral(ODataStrings.parseStringLiteral(text))

            case EdmPrimitiveTypeKind.Date => EsLiteral(LocalDate.parse(text))
            case EdmPrimitiveTypeKind.TimeOfDay => EsLiteral(LocalTime.parse(text))
            case EdmPrimitiveTypeKind.DateTimeOffset => EsLiteral(ZonedDateTime.parse(text))

            // TODO: bytes type

            case _ => ???
          }

        case _ => ???
      }
    }
  }

  override def visitUnaryOperator(operator: UnaryOperatorKind, operand: EsAstNode): EsAstNode = {
    operator match {
      case UnaryOperatorKind.NOT => EsNot(operand)
      case _ => ???
    }
  }

  override def visitBinaryOperator(operator: BinaryOperatorKind, left: EsAstNode, right: EsAstNode): EsAstNode = {
    operator match {
      case BinaryOperatorKind.EQ => EsEquals(left, right)
      case BinaryOperatorKind.NE => EsNot(EsEquals(left, right))
      case BinaryOperatorKind.GT => EsGreaterThan(left, right)
      case BinaryOperatorKind.GE => EsGreaterThanOrEqual(left, right)
      case BinaryOperatorKind.LT => EsLessThan(left, right)
      case BinaryOperatorKind.LE => EsLessThanOrEqual(left, right)
      case BinaryOperatorKind.AND => EsAnd(left, right)
      case BinaryOperatorKind.OR => EsOr(left, right)
      case _ => ???
    }
  }

  override def visitMethodCall(methodCall: MethodKind, parameters: java.util.List[EsAstNode]): EsAstNode = {
    (methodCall, parameters.asScala) match {

      case (MethodKind.CONTAINS, Seq(haystack, needle)) => EsContains(haystack, needle)
      case (MethodKind.STARTSWITH, Seq(haystack, prefix)) => EsStartsWith(haystack, prefix)
      case (MethodKind.ENDSWITH, Seq(haystack, suffix)) => EsEndsWith(haystack, suffix)

      // TODO: many more functions

      case _ => ???
    }
  }

  /**
    * Olingo search expressions are different from other expressions,
    * so this doesn't use the [[ExpressionVisitor]] interface.
    */
  def compileSearch(
    search: Option[SearchOption]
  ): Option[QueryDefinition] = {
    search
      .map(_.getSearchExpression)
      .map(translateSearchExpression)
      .map(QueryStringQueryDefinition(_))
  }

  /**
    * Escape reserved characters in ES query terms.
    *
    * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters
    * @throws IllegalArgumentException if the query contains characters that can't be escaped.
    */
  protected def escapeSearchTerm(term: String): String = {
    require(!(term.contains("<") || term.contains(">")),
      "ElasticSearch search terms cannot contain less than or greater than signs!")
    term
      .replaceAll("""[+\-=!()\[\]\^"~*?:\\/]""", """\\$0""")
      .replaceAll("""\|\|""", """\\|\\|""")
      .replaceAll("""\&\&""", """\\&\\&""")
  }

  /**
    * OData's search expression syntax is a subset of ES query string syntax.
    * Like OData, ES NOT binds tighter than AND, which binds tighter than OR.
    *
    * @see http://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part2-url-conventions/odata-v4.0-errata03-os-part2-url-conventions-complete.html#Search_Expression
    * @see https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#query-string-syntax
    */
  protected def translateSearchExpression(expr: SearchExpression): String = {
    expr match {

      case term: SearchTerm => escapeSearchTerm(term.getSearchTerm)

      case unary: SearchUnary => unary.getOperator match {
        case SearchUnaryOperatorKind.NOT =>
          s"NOT ${translateSearchExpression(unary.getOperand)}"
        case _ => ???
      }

      case binary: SearchBinary => binary.getOperator match {
        case SearchBinaryOperatorKind.AND =>
          s"${translateSearchExpression(binary.getLeftOperand)} AND ${translateSearchExpression(binary.getRightOperand)}"
        case SearchBinaryOperatorKind.OR =>
          s"${translateSearchExpression(binary.getLeftOperand)} OR ${translateSearchExpression(binary.getRightOperand)}"

        case _ => ???
      }

      case _ => ???
    }
  }
}

/**
  * ES queries are much less powerful than SQL, so only a small subset of OData expressions can be translated.
  */
sealed trait EsAstNode

case class EsField(name: String) extends EsAstNode

case class EsLiteral(value: Any) extends EsAstNode

case class EsEquals(left: EsAstNode, right: EsAstNode) extends EsAstNode

case class EsGreaterThan(left: EsAstNode, right: EsAstNode) extends EsAstNode

case class EsGreaterThanOrEqual(left: EsAstNode, right: EsAstNode) extends EsAstNode

case class EsLessThan(left: EsAstNode, right: EsAstNode) extends EsAstNode

case class EsLessThanOrEqual(left: EsAstNode, right: EsAstNode) extends EsAstNode

case class EsNot(operand: EsAstNode) extends EsAstNode

case class EsAnd(left: EsAstNode, right: EsAstNode) extends EsAstNode

case class EsOr(left: EsAstNode, right: EsAstNode) extends EsAstNode

case class EsContains(haystack: EsAstNode, needle: EsAstNode) extends EsAstNode

case class EsStartsWith(haystack: EsAstNode, prefix: EsAstNode) extends EsAstNode

case class EsEndsWith(haystack: EsAstNode, suffix: EsAstNode) extends EsAstNode
