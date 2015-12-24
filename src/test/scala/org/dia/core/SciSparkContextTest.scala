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
import org.dia.TestEnvironment.SparkTestConstants
import org.scalatest.FunSuite

/**
 * Tests for creating RDDs from different api calls.
 */
class SciSparkContextTest extends FunSuite {

  val sc = SparkTestConstants.sc
  val TestLinks = SparkTestConstants.datasetPath

  test("NetcdfFile.Local") {
    sc.setLocalProperty(ARRAY_LIB, ND4J_LIB)
    val variable = SparkTestConstants.datasetVariable
    val nd4jRDD: SRDD[SciTensor] = sc.NetcdfFile(TestLinks, List(variable))

    val smoothRDD = nd4jRDD.map(p => p(variable).reduceResolution(5, -9999))
    val collect: Array[SciTensor] = smoothRDD.map(p => p <= 241.0).collect()

    collect.foreach(sciTensor => {
      for (i <- 0 to sciTensor.tensor.rows - 1) {
        for (j <- 0 to sciTensor.tensor.cols - 1) {
          val ij = sciTensor.tensor(i, j)
          if (ij > 241.0) assert(false, "Indices : (" + i + "," + j + ") has value " + ij + "which is greater than 241.0")
        }
      }
    })

    assert(true)
  }

  test("NetcdfDFSFile.Local") {
    val variable = "data"
    val t = sc.NetcdfDFSFile("src/test/resources/Netcdf", List(variable))
    t.collect.map(p => {
      println(p)
      println(p.tensor)
    })
    assert(true)
  }
}