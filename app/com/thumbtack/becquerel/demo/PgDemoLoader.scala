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

package com.thumbtack.becquerel.demo

import java.io.File
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.{Duration, FiniteDuration}

/**
  * Load the Dell DVD Store data files into a PostgreSQL database.
  * Uses the provided loader script, provided it's been unpacked in the right place.
  *
  * @note The provided loader script only works if PG's running on this machine.
  *
  * @note The script is messy, so expect some nonfatal errors:
  *       • Postgres 10 and up don't have the createlang command
  *       • We need to create the ds2 user but the script creates it again
  *       • Some relations and roles won't exist until after the script is run once
  */
object PgDemoLoader {

  def main(args: Array[String]): Unit = {
    // Create the ds2 user used by the loader script.
    new ProcessBuilder()
      .command("psql", "--dbname", "postgres", "--command", "CREATE USER ds2 WITH SUPERUSER;")
      .inheritIO()
      .start()
      .waitFor(SharedConfig.timeout.toNanos, TimeUnit.NANOSECONDS)

    // Run the provided loader script.
    val loaderScriptDir = s"${SharedConfig.dvdStoreDir}/pgsqlds2"
    val loaderScript = s"$loaderScriptDir/pgsqlds2_create_all.sh"
    new ProcessBuilder()
      .command("/bin/sh", new File(loaderScript).getAbsoluteFile.toString)
      .directory(new File(loaderScriptDir).getAbsoluteFile)
      .inheritIO()
      .start()
      .waitFor(SharedConfig.timeout.toNanos, TimeUnit.NANOSECONDS)
  }
}
