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
import org.dia.Constants._
import org.dia.testenv.SparkTestConstants
import org.dia.core.{ SRDD, SciTensor }
import org.dia.loaders.NetCDFReader._
import org.dia.partitioners.SPartitioner._
import org.scalatest.FunSuite

/**
 * Tests NetCDF loaders from URIs, from local FS.
 */
class LoadersTest extends FunSuite {

  /**
   * Tests whether recursiveListFiles lists all files in all sub-directories.
   */
  test("RecursiveFileListing") {
    val path = "src/main/scala/"
    val dirsWithFiles = PathReader.recursiveListFiles(new File(path))
    println("Found: %d sub-directories.".format(dirsWithFiles.size))
    dirsWithFiles.foreach({
      case (dir, files) => {
        if (files.length > 0) {
          files.foreach(println)
          println()
        } else {
          println("Empty")
        }
      }
    })
    assert(true)
  }

  /**
   * Tests SRDD creation if NetCDFs are the files within a provided local directory.
   */
  test("LoadPathGrouping") {
    val dataUrls = List("src/test/resources/Netcdf")
    val sc = SparkTestConstants.sc.sparkContext
    sc.getConf.set("log4j.configuration", "resources/log4j-defaults.properties")
    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)
    val sBreezeRdd = new SRDD[SciTensor](sc, dataUrls, List("data"), loadNetCDFNDVar, mapSubFoldersToFolders)
    sBreezeRdd.collect()
    assert(true)
  }

  /**
   * Tests SRDD creation if NetCDFs are the files within a provided local directory,
   * but this time through the higher-level SciSparkContext.
   */
  test("OpenLocalPath") {
    val sc = SparkTestConstants.sc
    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)
    val path = "src/test/resources/Netcdf"
    val pathRDD: SRDD[SciTensor] = sc.OpenPath(path, List("data"))
    println(pathRDD.collect()(0).variables("data").data.length)
    assert(true)
  }

  /**
   * Tests SRDD creation if NetCDFs come from URIs provided in a local file.
   * This test here is in fact not loading any variables.
   */
  test("LoadMultiVariable") {
    val sc = SparkTestConstants.sc
    sc.setLocalProperty(ARRAY_LIB, ND4J_LIB)
    val path = SparkTestConstants.datasetPath
    val pathRDD = sc.NetcdfFile(path, List("data"))
    val tensors = pathRDD.collect().toList
    println("Number of tensors loaded " + tensors.length)
    println(tensors.toString())
    println("DONEDONEDONE")
    assert(true)
  }

}
