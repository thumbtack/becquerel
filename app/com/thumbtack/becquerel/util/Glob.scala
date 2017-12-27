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

import java.util.regex.Pattern

import scala.util.matching.Regex

/**
  * Compile glob patterns to regexes. Supports * and ? only.
  */
object Glob {

  def translate(glob: String): String = {
    val builder = StringBuilder.newBuilder
    var literalStart = -1
    glob.view.zipWithIndex.foreach {
      case (c, pos) if c == '*' || c == '?' =>
        if (literalStart != -1) {
          builder ++= Pattern.quote(glob.substring(literalStart, pos))
          literalStart = -1
        }
        builder ++= (c match {
          case '*' => ".*"
          case '?' => "."
        })
      case (_, pos) =>
        if (literalStart == -1) {
          literalStart = pos
        }
    }
    if (literalStart != -1) {
      builder ++= Pattern.quote(glob.substring(literalStart, glob.length))
    }
    builder.result()
  }

  def compile(glob: String): Regex = {
    translate(glob).r
  }

  def compile(globs: TraversableOnce[String]): Regex = {
    globs
      .map(glob => s"(?:${translate(glob)})")
      .mkString("|")
      .r
  }

  implicit class RegexExtensions(r: Regex) {
    def matches(s: String): Boolean = r.unapplySeq(s).nonEmpty
  }
}
