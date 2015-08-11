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

import breeze.linalg.DenseMatrix
import org.dia.sLib.mccOps
import org.dia.tensors.{BreezeTensor, Nd4jTensor}
import org.nd4j.api.Implicits._
import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite
/**
 * Mesoscale convective complex (MCC) test
 */
class MCCAlgorithmTest extends FunSuite {


  test("reduceResolutionTest") {
    val dense = new DenseMatrix[Double](180, 360, (1d to 64800d by 1d).toArray, 0, 360, true)
    val nd = Nd4j.create((1d to 64800d by 1d).toArray, Array(180, 360))
    val breeze = new BreezeTensor(dense)
    val nd4j = new Nd4jTensor(nd)

    val b = mccOps.reduceResolution(breeze, 90) <= 241.0
    val n = mccOps.reduceResolution(nd4j, 90) <= 241.0

    println(b)
    println(n)

    println(b.data.toList)
    println(n.data.toList)

    //    println(breeze.compData.toList)
    //    println(nd4j.compData.toList)
  }

  test("filter") {
//    val dense = Nd4j.create(Array[Double](1, 241, 241, 1), Array(2, 2))
//    val t = dense.map(p => if (p < 241.0) p else 0.0)
//    println(t)
    assert(true)
  }

}
