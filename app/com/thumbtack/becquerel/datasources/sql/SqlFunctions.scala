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

import org.apache.calcite.sql.{SqlKind, SqlPrefixOperator}
import org.apache.calcite.sql.`type`.{InferTypes, OperandTypes, ReturnTypes}

object SqlFunctions {
  /**
    * Pseudo-operator used to force parentheses around function calls that are emulated by other expressions.
    */
  val FORCE_PARENS: SqlPrefixOperator = new SqlPrefixOperator(
    "", // Doesn't generate SQL other than the wrapped expression.
    SqlKind.OTHER,
    0, // Lowest precedence.
    ReturnTypes.ARG0,
    InferTypes.RETURN_TYPE,
    OperandTypes.ANY
  )
}
