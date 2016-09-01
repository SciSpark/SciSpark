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
package org.dia.core

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.dia.Constants._
import org.dia.testenv.SparkTestConstants

/**
 * Tests for creating SRDDs from different sources.
 */
class SciSparkContextTest extends FunSuite with BeforeAndAfter {

  val sc = SparkTestConstants.sc
  val testLinks = SparkTestConstants.datasetPath
  val username = SparkTestConstants.testHTTPCredentials("username")
  val password = SparkTestConstants.testHTTPCredentials("password")
  before {
    sc.addHTTPCredential("http://disc2.gesdisc.eosdis.nasa.gov:80", username, password)
  }

  /**
   * Creates SRDD from a file of URIs.
   */
  test("NetcdfFile.Local") {
    sc.setLocalProperty(ARRAY_LIB, ND4J_LIB)
    val variable = SparkTestConstants.datasetVariable
    val sRDD = sc.netcdfFileList(testLinks, List(variable))

    val smoothedSRDD = sRDD.map(p => p(variable).reduceResolution(5, -9999))
    val collect = smoothedSRDD.map(_ <= 241.0).collect()

    collect.foreach(t => {
      for (i <- 0 until t.tensor.rows) {
        for (j <- 0 until t.tensor.cols) {
          if (t.tensor(i, j) > 241.0) {
            assert(false, "Indices : (" + i + "," + j + ") has value " + t.tensor(i, j) + "which is greater than 241.0")
          }
        }
      }
    })
    assert(true)
  }

  /**
   * Creates SRDD from a directory with NetCDF files in it.
   */
  test("NetcdfDFSFile.Local") {
    val variable = "data"
    val rdd = sc.netcdfDFSFiles("src/test/resources/Netcdf/", List(variable))
    val collected = rdd.collect

    assert(collected.length == 2)
  }

  /**
   * Creates SRDD from a directory with NetCDF files in it.
   */
  test("NetcdfRandomAccessFile") {
    val variable = "data"
    val rdd = sc.netcdfRandomAccessDatasets("src/test/resources/Netcdf/", List(variable))
    val count = rdd.count()

    assert(count == 2)
  }

  test("sciDatasets") {
    val variable = "data"
    val variable2 = SparkTestConstants.datasetVariable
    val rdd = sc.sciDatasets("src/test/resources/Netcdf/", List(variable))
    val listRDD = sc.sciDatasets("src/test/resources/TestLinks2.txt", List(variable2))
    val count = rdd.count()
    val listCount = listRDD.count()
    assert(count == 2)
    assert(listCount == 2)
  }

  test("PartitionCount") {
    val variable = "data"
    val variable2 = SparkTestConstants.datasetVariable
    val rdd = sc.sciDatasets("src/test/resources/Papersize/", List(variable), 4)
    val nrdd = sc.netcdfDFSFiles("src/test/resources/Papersize/", List(variable), 4)
    assert(rdd.getNumPartitions == 4)
  }
}
