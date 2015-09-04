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
import org.dia.TestEnvironment.SparkTestConstants
import org.dia.core.{sRDD, sciTensor}
import org.dia.loaders.NetCDFReader._
import org.dia.partitioners.sPartitioner._

/**
 * Tests for grouping from system paths.
 */
class LoadersTest extends org.scalatest.FunSuite {

  test("RecursiveFileListing") {
    val path = "src/main/scala/"
    val files = PathReader.recursiveListFiles(new File(path))
    println("Found: %d sub-directories.".format(files.size))
    files.foreach(vals => {
      if (vals._2.length > 0) {
        vals._2.foreach(e => println(e))
        println()
      } else {
        println("Empty")
      }
    })
    assert(true)
  }

  test("LoadPathGrouping") {
    val dataUrls = List("/Users/marroqui/Documents/projects/scipark/compData/TRMM_3Hourly_3B42_1998/")
    val sc = SparkTestConstants.sc.sparkContext
    sc.getConf.set("log4j.configuration", "resources/log4j-defaults.properties")
    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)

    val sBreezeRdd = new sRDD[sciTensor](sc, dataUrls, List("precipitation"), loadNetCDFNDVars, mapSubFoldersToFolders)
    sBreezeRdd.collect()
    assert(true)
  }

  test("OpenLocalPath") {
    val sc = SparkTestConstants.sc
    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)
    val path = "/Users/marroqui/Documents/projects/scipark/compData/TRMM_3Hourly_3B42_1998/"
    val pathRDD: sRDD[sciTensor] = sc.OpenPath(path, List("precipitation"))
    println(pathRDD.collect()(0).variables("precipitation").data.length)
    assert(true)
  }

  test("LoadMultiVariable") {
    val sc = SparkTestConstants.sc
    sc.setLocalProperty(ARRAY_LIB, ND4J_LIB)
    val path = "TestLinks2"
    //val variables = List("TotalCounts_A", "TotCldLiqH2O_A", "TotCldLiqH2O_A_ct")
    val pathRDD: sRDD[sciTensor] = sc.NetcdfFile(path)
    val t = pathRDD.collect().toList
    println("Number loaded " + t.length)
    println(t.toString())
    println("DONEDONEDONE")
    assert(true)
  }
}
