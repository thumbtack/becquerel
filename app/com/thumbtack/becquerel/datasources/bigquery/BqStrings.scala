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

object BqStrings {
  /**
    * Escape LIKE wildcards in a string.
    *
    * @see https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#comparison-operators
    */
  def escapeForLike(s: String): String = {
    s.replaceAll("[%_]", "\\\\\\\\$0")
  }

  /**
    * Can we put this string in a BigQuery single-line comment?
    *
    * @note Intentionally conservative because it's not clear what BigQuery thinks is a line ending.
    *
    * @see https://cloud.google.com/bigquery/docs/reference/standard-sql/lexical#single-line-comments
    * @see http://www.unicode.org/versions/Unicode9.0.0/ch05.pdf#G10213
    */
  def isSingleLineCommentSafe(s: String): Boolean = {
    !s.exists(c => c.isControl || c == '\u2028' || c == '\u2029')
  }
}
