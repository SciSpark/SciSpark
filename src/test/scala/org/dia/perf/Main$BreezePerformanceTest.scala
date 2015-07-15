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

package org.dia.perf

import breeze.linalg.{DenseMatrix, DenseVector}
import org.scalatest.{FunSuite, Ignore}

/**
 * This is a scala breeze implementation of the
 * metrics test in ocw. The purpose is to
 * test the performance of simple biasing functions
 *
 * Source : https://github.com/apache/climate/blob/master/ocw/metrics.py
 * Created by rahulsp on 6/22/15.
 */
@Ignore
class Main$BreezePerformanceTest extends FunSuite {

  // Files URL
  val FILE_URL = "http://zipper.jpl.nasa.gov/dist/"
  // Two Local Model Files
  val FILE_1 = "AFRICA_KNMI-RACMO2.2b_CTL_ERAINT_MM_50km_1989-2008_tasmax.nc"
  val FILE_2 = "AFRICA_UC-WRF311_CTL_ERAINT_MM_50km-rg_1989-2008_tasmax.nc"
  val NANO_SECS = 1000000000.0

  test("ocwMetricsBreezeTest") {
//    val knmi_dataset = Main.getBreezeNetCDFNDVars(FILE_URL + FILE_1, "tasmax")
//    val wrf_dataset = Main.getBreezeNetCDFNDVars(FILE_URL + FILE_2, "tasmax")
//    val result = new Array[DenseMatrix[Double]](knmi_dataset.length)
//    println("Total Running time:\n")
//    val totalBefore = System.nanoTime()
//    for (i <- 0 to knmi_dataset.length - 1) {
//      result(i) = knmi_dataset(i) - wrf_dataset(i)
//    }
//    val totalAfter = System.nanoTime()
//    println((totalAfter - totalBefore)/NANO_SECS)
    assert(true)
  }

  test("ocwMetricsBreezeIterationTest") {
    //    val knmi_dataset = Main.getBreezeNetCDFNDVars(FILE_LEADER + FILE_1, "tasmax")
    //    val wrf_dataset = Main.getBreezeNetCDFNDVars(FILE_LEADER + FILE_2, "tasmax")
    //    val result = new Array[DenseMatrix[Double]](knmi_dataset.length)
    //    println("Running times per iteration:\n")
    //    for(i <- 0 to knmi_dataset.length - 1){
    //      val before = System.nanoTime()
    //      result(i) = knmi_dataset(i) - wrf_dataset(i)
    //      val after = System.nanoTime()
    //       print in seconds
    //      println((after - before)/NANO_SECS)
    //    }
    assert(true)
  }

  test("Multiple Slicing") {
    (1 to 100).foreach { i =>
      val m = DenseMatrix.zeros[Double](i * 1000, i * 1000) //, (1000000 to 2000000).toArray.map(p => p.toDouble), 0)
    val m2 = DenseMatrix.ones[Double](i * 1000, i * 1000) //, (1 to 1000000).toArray.map(p => p.toDouble), 0)
    val start = System.nanoTime()
      //val m3 = m * m2
      val m3 = m :* m2
      val stop = System.nanoTime()
      println(stop - start)
      val slice1 = m(1 to 3, 1 to 3)
      assert(slice1(::, 1) == DenseVector(14, 15, 16))
      assert(slice1(::, 1 to 2) == DenseMatrix((14, 20), (15, 21), (16, 22)))
    }
  }

}
