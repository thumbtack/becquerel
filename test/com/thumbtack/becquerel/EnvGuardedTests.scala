/*
 *    Copyright 2017–2018 Thumbtack
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

package com.thumbtack.becquerel

import org.scalatest._

object EnvGuard {
  def isEnvVarTrue(envVarName: String): Boolean = {
    Option(System.getenv(envVarName)).exists(_.toBoolean)
  }

  def isEnvVarFalse(envVarName: String): Boolean = {
    Option(System.getenv(envVarName)).forall(!_.toBoolean)
  }
}

trait EnvGuardedTests extends FunSuite {

  /**
    * Run a test only if an environment variable is present and equal to the string "true".
    */
  def envGuardedTest(envVarNames: String*)(testName: String, testTags: Tag*)(testFun: => Unit): Unit = {
    if (envVarNames.forall(EnvGuard.isEnvVarTrue)) {
      test(testName, testTags: _*)(testFun)
    } else {
      ignore(testName, testTags: _*)(())
    }
  }

  /**
    * Run a test only if an environment variable is absent or equal to the string "false".
    */
  def envBlockedTest(envVarNames: String*)(testName: String, testTags: Tag*)(testFun: => Unit): Unit = {
    if (envVarNames.forall(EnvGuard.isEnvVarFalse)) {
      test(testName, testTags: _*)(testFun)
    } else {
      ignore(testName, testTags: _*)(())
    }
  }
}

trait EnvGuardedSuite extends TestSuiteMixin { this: TestSuite =>

  def guardEnvVarNames: Seq[String]

  def blockEnvVarNames: Seq[String]

  abstract override def run(testName: Option[String], args: Args): Status = {
    if (guardEnvVarNames.forall(EnvGuard.isEnvVarTrue) && blockEnvVarNames.forall(EnvGuard.isEnvVarFalse)) {
      super.run(testName, args)
    } else {
      SucceededStatus
    }
  }
}
