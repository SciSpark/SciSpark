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
package org.dia.MCC

import breeze.linalg.DenseMatrix
import org.dia.algorithms.mcc.mccOps
import org.dia.tensors.{BreezeTensor, Nd4jTensor}
import org.nd4j.api.Implicits._
import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite

/**
 * Mesoscale convective complex (MCC) test
 */
class MCCAlgorithmTest extends FunSuite {


  /**
   * Nd4s slicing is broken at the moment
   */
  test("reduceResolutionTest") {
    val dense = new DenseMatrix[Double](4, 2, (0d to 8d by 1d).toArray, 0, 2, true)
    val nd = Nd4j.create((0d to 8d by 1d).toArray, Array(4, 2))
    val breeze = new BreezeTensor((0d to 8d by 1d).toArray, Array(4, 2))
    val nd4j = new Nd4jTensor(nd)

    println("breeze")
    val b = mccOps.reduceResolution(breeze, 2, 999999)
    println("nd4j")
    val n = mccOps.reduceResolution(nd4j, 2, 999999)


    println(breeze)
    println(nd4j)

    if (breeze == nd4j) println("THESE ARE TRUE TRUE TRUE")
    println(b)
    println(n)

    println(b.data.toList)
    println(n.data.toList)

    assert(b == n)
  }

  test("filter") {
    val dense = Nd4j.create(Array[Double](1, 241, 241, 1), Array(2, 2))
    val t = dense.map(p => if (p < 241.0) p else 0.0)
    println(t)
    assert(true)
  }

  test("Nd4sSlice") {
    val nd = Nd4j.create((0d to 8d by 1d).toArray, Array(4, 2))
    println(nd)
    println(nd(0 -> 1, ->))
    assert(false)
  }

}
