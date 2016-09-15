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
package org.dia.mcc

import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite

import org.dia.algorithms.mcc.MCCOps
import org.dia.tensors.{AbstractTensor, BreezeTensor, Nd4jTensor}

/**
 * Tests functionality in MCCOpsTest including:
 * reduceResolution,labelConnectedComponents,findConnectedComponents.
 */
class MCCOpsTest extends FunSuite {

  /**
   * Note that Nd4s slicing is broken at the moment
   */
  test("reduceResolutionTest") {
    val breeze = new BreezeTensor((0d to 8d by 1d).toArray, Array(4, 2))
    val nd4j = new Nd4jTensor(((0d to 8d by 1d).toArray, Array(4, 2)))
    val breezeReduced = MCCOps.reduceResolution(breeze, 2, 999999)
    val nd4jReduced = MCCOps.reduceResolution(nd4j, 2, 999999)
    assert(breezeReduced == nd4jReduced)
    assert(breezeReduced.data.toList == List(1.5, 5.5))
  }

  test("testlabelConnectedComponents") {
    val m = Array(
      Array(1.0, 2.0, 0.0, 4.0),
      Array(5.0, 6.0, 0.0, 8.0),
      Array(43.9, 23.0, 1.0, 0.0),
      Array(0.0, 0.0, 0.0, 0.0),
      Array(1.0, 0.0, 1.0, 0.0))

    val cc = Array(
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 1.0, 0.0),
      Array(0.0, 0.0, 0.0, 0.0),
      Array(3.0, 0.0, 4.0, 0.0))

    val ndArray = Nd4j.create(m)
    val ccArray = Nd4j.create(cc)
    val t = new Nd4jTensor(ndArray)
    val cct = new Nd4jTensor(ccArray)
    val (labelledTensor, numComponent) = MCCOps.labelConnectedComponents(t)
    assert(numComponent == 4)
    assert(labelledTensor == cct)
  }

  test("reduceResRectangle") {

    val array = Array(
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 1.0, 0.0),
      Array(0.0, 0.0, 0.0, 0.0),
      Array(3.0, 0.0, 4.0, 0.0))

    val flattened = array.flatten
    val tensor: AbstractTensor = new Nd4jTensor(flattened, Array(5, 4))
    val averageColumnsolution = new Nd4jTensor(Array(1.2, 0.6, 1.0, 0.8), Array(1, 4))
    val averageDoubleColumnsSolution = new Nd4jTensor(Array(0.9, 0.9), Array(1, 2))
    val averageRowSolution = new Nd4jTensor(Array(1.0, 1.0, 0.75, 0.0, 1.75), Array(5, 1))
    val mismatchedDimensionSolution = new Nd4jTensor(Array(0.7777778), Array(1, 1))

    val averageColumns = MCCOps.reduceRectangleResolution(tensor, 5, 1, 9999999)
    val averageDoubleColums = MCCOps.reduceRectangleResolution(tensor, 5, 2, 9999999)
    val averageRows = MCCOps.reduceRectangleResolution(tensor, 1, 4, 999999)
    // need to round to hundredth as answer is rounded to hundredth
    val mismatchedDimension = MCCOps.reduceRectangleResolution(tensor, 3, 3, 99999999)

    assert(averageColumns == averageColumnsolution)
    assert(averageDoubleColums == averageDoubleColumnsSolution)
    assert(averageRows == averageRowSolution)
    assert(mismatchedDimension == mismatchedDimensionSolution)
  }

}
