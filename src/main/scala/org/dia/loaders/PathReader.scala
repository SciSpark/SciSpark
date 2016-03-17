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
package org.dia.loaders

import java.io.File
import org.slf4j.Logger

/**
 * Utility functions to get files (just names) from local file system.
 */
object PathReader {

  // Class logger
  val LOG: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Gets all files recursively starting at the path of a given file.
   *
   * @param f a specified file object serving as the root starting path
   * @return a map mapping a set of file names against paths
   */
  def recursiveListFiles(f: File): Map[String, Array[String]] = {
    if (!f.exists()) {
      LOG.error("%s not found!".format(f.getAbsolutePath))
      throw new IllegalArgumentException("%s not found!".format(f.getAbsolutePath))
    }
    val files = Map((f.getName, f.listFiles.filter(p => !p.isDirectory & !p.isHidden).map(f => f.getAbsolutePath)))
    val dirs = f.listFiles.filter(_.isDirectory)
    files ++ dirs.flatMap(recursiveListFiles)
  }

}