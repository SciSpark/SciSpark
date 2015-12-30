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
package org.dia.tensors

import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite
import org.nd4s.Implicits._

/**
 * Checks that for 2D arrays, using Breeze or Nd4j
 * leads to the same tensor.
 */
class ComparisonTest extends FunSuite {

  val ArraySample = Array(
    Array(0.0, 1.0, 0.0, 1.0, 1.0),
    Array(0.0, 1.0, 0.0, 2.0, 1.0),
    Array(2.0, 3.0, 4.0, 1.0, 2.0),
    Array(0.0, 1.0, 0.0, 1.0, 1.0),
    Array(0.0, 1.0, 0.0004003492849200234294, 2.0, 1.0),
    Array(2.0, 3.0, 4.0, 1.0, 2.0))

  val sample = Nd4j.create(ArraySample)
  val shapePair = (sample.data.asDouble, sample.shape)

  test("Testing Equality") {
    val nd4j: AbstractTensor = new Nd4jTensor(shapePair)
    val breeze: AbstractTensor = new BreezeTensor(shapePair)
    assert(nd4j.rows == breeze.rows)
    assert(nd4j.cols == breeze.cols)
    assert(nd4j == breeze)
  }

}
