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

package org.dia.tensors.perf

import breeze.linalg.DenseMatrix
import org.scalatest.FunSuite

/**
 * This is a scala breeze implementation of the
 * metrics test in ocw. The purpose is to
 * test the performance of simple biasing functions
 *
 * Source : https://github.com/apache/climate/blob/master/ocw/metrics.py
 * Created by rahulsp on 6/22/15.
 */
class MainMemory$BreezePerformanceTest extends FunSuite {

  // Files URL
  val FILE_URL = "http://zipper.jpl.nasa.gov/dist/"
  // Two Local Model Files
  val FILE_1 = "AFRICA_KNMI-RACMO2.2b_CTL_ERAINT_MM_50km_1989-2008_tasmax.nc"
  val FILE_2 = "AFRICA_UC-WRF311_CTL_ERAINT_MM_50km-rg_1989-2008_tasmax.nc"
  val NANO_SECS = 1000000000.0

  ignore("Breeze.element.wise.test") {
    println("Breeze.element.wise.test")
    (1 to 100).foreach { i =>
      val m1 = DenseMatrix.zeros[Double](i * 1000, i * 1000)
      val m2 = DenseMatrix.zeros[Double](i * 1000, i * 1000)
      /**
       * Vector subtraction
       */
      val start = System.nanoTime()
      val m3 = m1 - m2
      val stop = System.nanoTime()
      println(stop - start)
    }
    assert(true)
  }

  test("Breeze.vector.wise.test") {
    println("Breeze.vector.wise.test")
    (1 to 100).foreach { i =>
      val m1 = DenseMatrix.zeros[Double](i * 1000, i * 1000)
      val m2 = DenseMatrix.zeros[Double](i * 1000, i * 1000)

      val start = System.nanoTime()
      val m3 = m1 * m2
      val stop = System.nanoTime()
      println(stop - start)
    }
    assert(true)
  }

}
