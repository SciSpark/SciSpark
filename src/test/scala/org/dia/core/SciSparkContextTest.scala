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

import org.dia.Constants._
import org.dia.testenv.SparkTestConstants
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

/**
 * Tests for creating SRDDs from different sources.
 */
class SciSparkContextTest extends FunSuite with BeforeAndAfter {

  val sc = SparkTestConstants.sc
  val testLinks = SparkTestConstants.datasetPath

  before {
    sc.addHTTPCredential("http://disc2.gesdisc.eosdis.nasa.gov:80", "scispark", "SciSpark1")
  }

  /**
   * Creates SRDD from a file of URIs.
   */
  test("NetcdfFile.Local") {
    sc.setLocalProperty(ARRAY_LIB, ND4J_LIB)
    val variable = SparkTestConstants.datasetVariable
    val sRDD = sc.NetcdfFile(testLinks, List(variable))

    val smoothedSRDD = sRDD.map(p => p(variable).reduceResolution(5, -9999))
    val collect = smoothedSRDD.map(_ <= 241.0).collect()

    collect.foreach(t => {
      for (i <- 0 to t.tensor.rows - 1) {
        for (j <- 0 to t.tensor.cols - 1) {
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
    val rdd = sc.NetcdfDFSFile("src/test/resources/Netcdf", List(variable))
    val collected = rdd.collect

    assert(collected.length == 2)
  }

}