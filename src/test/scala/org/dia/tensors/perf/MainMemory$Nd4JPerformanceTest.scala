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


import org.nd4j.linalg.factory.Nd4j
import org.nd4s.Implicits._
import org.scalatest.{FunSuite, Ignore}
/**
 * The Nd4j Performance Tests
 * Created by rahulsp on 7/7/15.
 */
@Ignore
class MainMemory$Nd4JPerformanceTest extends FunSuite {

  test("ND4J.element.wise.test") {
    println("ND4J.element.wise.test")
    (1 to 100).foreach { p =>
      val m1 = Nd4j.create(p * 1000 * p * 1000).reshape(p * 1000, p * 1000)
      val m2 = Nd4j.create(p * 1000 * p * 1000).reshape(p * 1000, p * 1000)
      /**
       * Vector subtraction
       */
      val start = System.nanoTime()
      val m3 = m1 - m2
      val stop = System.nanoTime()
      println("Using blas wrapper calls : " + (stop - start))
      val start2 = System.nanoTime()
      for (row <- 0 to m1.rows - 1) {
        for (col <- 0 to m1.columns - 1) {
          m3.put(row, col, m1(row, col) - m2(row, col))
        }
      }
      val stop2 = System.nanoTime()
      println("Using a looped subtractin : " + (stop2 - start2))
    }
    assert(true)
  }

  test("ND4J.vector.wise.test") {
    println("ND4J.vector.wise.test")
    (1 to 100).foreach { p =>
      val m1 = Nd4j.create(p * 1000 * p * 1000).reshape(p * 1000, p * 1000)
      val m2 = Nd4j.create(p * 1000 * p * 1000).reshape(p * 1000, p * 1000)
      /**
       * Vector subtraction
       */
      val start = System.nanoTime()
      val m3 = m1 dot m2
      val stop = System.nanoTime()
      println(stop - start)
    }
    assert(true)
  }

}
