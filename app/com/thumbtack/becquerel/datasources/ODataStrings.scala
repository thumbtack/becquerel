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

package com.thumbtack.becquerel.datasources

object ODataStrings {
  /**
    * @see https://docs.oasis-open.org/odata/odata/v4.0/errata03/os/complete/part3-csdl/odata-v4.0-errata03-os-part3-csdl-complete.html#_SimpleIdentifier
    *
    * @note Not reversible.
    */
  def sanitizeName(name: String): String = {
    name.replaceAll("""\W+""", "_").ensuring(_.length <= 128)
  }

  /**
    * Used to join sanitized name fragments into names.
    *
    * For example, the struct-typed column `tt-dp-prod.events.enriched.id`
    * would become the type `BigQuery.tt_dp_prod\_\_events\_\_enriched\_\_id`
    * and the entity set `BigQuery.Data.tt_dp_prod\_\_events\_\_enriched\_\_id`.
    *
    * @note Not significant, just slightly easier to reverse.
    */
  val idSep = "__"

  /**
    * Parse an OData URL string literal.
    */
  def parseStringLiteral(s: String): String = {
    assert(s.length >= 2)
    assert(s.startsWith("'"))
    assert(s.endsWith("'"))
    s
      .substring(1, s.length - 1)
      .replace("''", "'")
  }

  /**
    * Unparse an OData URL string literal.
    */
  def unparseStringLiteral(s: String): String = {
    s"'${s.replace("'", "''")}'"
  }
}
