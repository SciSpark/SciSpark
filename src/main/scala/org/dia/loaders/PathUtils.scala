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
 * Contains all functions needed to handle netCDF files
 */
object PathUtils {

  // Class logger
  val LOG: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def mapSubFoldersToFolders(paths: List[String]): List[List[String]] = {
    var fileList : List[List[String]] = List()
    for ( path <- paths) {
      val files = recursiveListFiles(new File(path))
      fileList = fileList ++ files.map(e => e._2.toList).toList
    }
    fileList
  }

  def recursiveListFiles(f: File): Map[String, Array[String]] = {
    if (!f.exists()) {
      LOG.error("%s not found!".format(f.getAbsolutePath))
      throw new IllegalArgumentException("%s not found!".format(f.getAbsolutePath))
    }
    val dirFiles: Map[String, Array[String]] = Map((f.getName, f.listFiles.filter(p => (!(p.isDirectory) & !(p.isHidden))).map(f => f.getAbsolutePath)))
    val these = f.listFiles
    dirFiles ++ these.filter(_.isDirectory).flatMap(recursiveListFiles)
  }
}