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

import org.apache.calcite.config.NullCollation
import org.apache.calcite.sql.SqlDialect
import org.apache.calcite.sql.SqlDialect.DatabaseProduct

/**
  * Describe to Calcite how to serialize a SQL AST so that BigQuery can read it.
  */
object BqDialect extends SqlDialect(
  DatabaseProduct.UNKNOWN,
  "BigQuery standard SQL",
  "`",
  NullCollation.LOW
) {
  /**
    * Use `OFFSET x LIMIT y` instead of `OFFSET x ROWS FETCH NEXT y ROWS ONLY`.
    */
  override def supportsOffsetFetch(): Boolean = false
}
