/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dia.utils

import scala.io.Source

/**
 * Utilities related to downloading from OpenDap server.
 */
object OpenDapUtils {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  /**
   * Read the user credentials from a file. Assumes the credentials are in the file in the format
   * username XXXXX
   * password XXXXX
   * @param  cFile is the path to the file (on local FS) containing the user credentials
   *               The default location to get the credentials is from the test/resources
   * @return A tuple of the username and the password
   */
  def getLoginCredentials(
      cFile: String = "src/test/resources/TestHTTPCredentials"): (String, String) = {
    var username = ""
    var password = ""
    try{
      val cList =
        Source.fromFile(cFile)
          .getLines()
          .map(p => {
          val split = p.split("\\s+")
          (split(0), split(1))
          }).toList
      username = cList(0)._2
      password = cList(1)._2
    }
    catch {
      case _: Throwable => logger.info("Problem with credentials at " + cFile + "\n")
    }
    (username, password)
  }
}
