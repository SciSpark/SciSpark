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

import breeze.linalg.DenseMatrix
import org.dia.algorithms.mcc.MCCOps
import org.dia.tensors.{ AbstractTensor, BreezeTensor, Nd4jTensor }
import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite

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

  test("testFindConnectedComponents") {
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
    val labelled = MCCOps.labelConnectedComponents(t)
    println(labelled)
    assert(labelled._1.equals(cct))
  }

  test("reduceResRectangle") {

    val m = Array(
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 1.0, 0.0),
      Array(0.0, 0.0, 0.0, 0.0),
      Array(3.0, 0.0, 4.0, 0.0))

    val k = m.flatMap(p => p)
    val ndArray = new DenseMatrix(5, 4, k, 0, 4, true)
    val t: AbstractTensor = new BreezeTensor(ndArray)
    println()
    println(MCCOps.reduceResolution(t, 5, 1))
    val reduced = MCCOps.reduceRectangleResolution(t, 3, 3, 99999999)
    println(reduced)
  }

  test("findComponents") {
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
    val frames = MCCOps.findConnectedComponents(t)
    val ccframes = MCCOps.findConnectedComponents(cct)
    assert(frames == ccframes)
  }

}
