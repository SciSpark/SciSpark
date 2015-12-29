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
package org.dia.partitioners

import java.io.File
import org.dia.loaders.PathReader.recursiveListFiles
import scala.language.implicitConversions

/**
 * Functions needed to split input paths into groups.
 */
object SPartitioner {

  /**
   * Each input path is its own group.
   */
  def mapOneUrl(uris: List[String]): List[List[String]] = {
    uris.map(elem => List(elem))
  }

  /**
   * Partitions input paths in fixed sized groups.
   */
  def mapNUri(N: Int)(uris: List[String]): List[List[String]] = {
    uris.grouped(N).toList
  }

  /**
   * Maps folders to lists containing their files.
   *
   * @param paths List of folders
   */
  def mapSubFoldersToFolders(paths: List[String]): List[List[String]] = {
    var fileList: List[List[String]] = List()
    for (path <- paths) {
      val files = recursiveListFiles(new File(path))
      fileList = fileList ++ files.map(e => e._2.toList).toList
    }
    fileList
  }

}

