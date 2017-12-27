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

package com.thumbtack.becquerel.util

import org.apache.calcite.sql.parser.SqlParserPos
import org.apache.calcite.sql.{SqlBasicCall, SqlNode, SqlOperator}

/**
  * Convenience functions for building SQL ASTs with Calcite.
  */
object CalciteExtensions {
  implicit class SqlOperatorExtensions(operator: SqlOperator) {
    /**
      * Generate node for a function or operator invocation.
      */
    def apply(operands: SqlNode*): SqlNode = {
      operator.createCall(SqlParserPos.ZERO, operands: _*)
    }
  }
}
