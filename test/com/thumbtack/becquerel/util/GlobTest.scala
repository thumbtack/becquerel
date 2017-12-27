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

import org.scalatest.FunSuite

import com.thumbtack.becquerel.util.Glob._

class GlobTest extends FunSuite {

  test("translate") {
    assert(Glob.translate("") === """""")
    assert(Glob.translate("*") === """.*""")
    assert(Glob.translate("**") === """.*.*""")
    assert(Glob.translate("?") === """.""")
    assert(Glob.translate("??") === """..""")
    assert(Glob.translate("becquerel__*") === """\Qbecquerel__\E.*""")
    assert(Glob.translate("*-tmp") === """.*\Q-tmp\E""")
    assert(Glob.translate("*_market_*") === """.*\Q_market_\E.*""")
    assert(Glob.translate("data_*_201?") === """\Qdata_\E.*\Q_201\E.""")
    assert(Glob.translate("a+b") === """\Qa+b\E""")
  }

  test("compile single") {
    assert(Glob.compile("becquerel__*").matches("becquerel__test"))
    assert(Glob.compile("*-tmp").matches("viper-tmp"))
    assert(Glob.compile("*_market_*").matches("underground_market_leaders"))
    assert(Glob.compile("data_*_201?").matches("data_nj_2017"))
  }

  test("compile multiple") {
    val r = Glob.compile(Seq("becquerel__*", "experimental__*"))
    assert(r.matches("becquerel__test"))
    assert(r.matches("experimental__defense_systems"))
    assert(!r.matches("obsolete__satellite_uplink"))
  }
}
