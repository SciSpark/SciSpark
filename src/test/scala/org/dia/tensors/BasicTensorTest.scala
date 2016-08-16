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

import org.dia.loaders.TestMatrixReader._

/**
 *
 * Tests basic tensor functionality.
 */
class BasicTensorTest extends FunSuite {


  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   *
   * Test statistical operations
   */
  test("mean") {
    logger.info("In mean test ...")
    val array = randVar
    val flattened = array.flatten
    val cascadedArray = flattened ++ flattened ++ flattened
    val square = Nd4j.create(array)

    val squareTensor = new Nd4jTensor(square)
    logger.info("The square shape is " + squareTensor.shape.toList)
    val cubeTensor = new Nd4jTensor((cascadedArray, Array(3) ++ squareTensor.shape))

    val averagedCube = cubeTensor.mean(0)

    assert(averagedCube == squareTensor)
  }

  /**
   *
   * Test relational operators
   */
  test("reshape") {
    logger.info("In reshape test ...")
    val cube = Nd4j.create((1d to 16d by 1d).toArray, Array(2, 2, 2, 2))
    val square = Nd4j.create((1d to 16d by 1d).toArray, Array(4, 4))
    val cubeTensor = new Nd4jTensor(cube)
    val squareTensor = new Nd4jTensor(square)
    val reshapedcubeTensor = cubeTensor.reshape(Array(4, 4))
    assert(reshapedcubeTensor == squareTensor)
  }

  test("filter") {
    logger.info("In filter test ...")
    val dense = Nd4j.create(Array[Double](1, 241, 241, 1), Array(2, 2))
    val t = Nd4j.create(Array[Double](1, 0, 0, 0), Array(2, 2))
    val Nd4jt1 = new Nd4jTensor(t)
    val Nd4jt2 = new Nd4jTensor(dense).map(p => if (p < 241) p else 0)
    logger.info(t.toString)
    assert(Nd4jt1 == Nd4jt2)
  }

  test("lessThanMask") {
    val dense = Nd4j.create(Array[Double](1, 241, 241, 242), Array(2, 2))
    val t = Nd4j.create(Array[Double](1, 0, 0, 0), Array(2, 2))
    val Nd4jt1 = new Nd4jTensor(t)
    val Nd4jt2 = new Nd4jTensor(dense)
    val filteredNd4jt2 = Nd4jt2 < 241.0
    assert(Nd4jt1 == filteredNd4jt2)
  }

  test("lessThanOrEqualToMask") {
    val dense = Nd4j.create(Array[Double](1, 241, 242, 1), Array(2, 2))
    val t = Nd4j.create(Array[Double](1, 241, 0, 1), Array(2, 2))
    val Nd4jt1 = new Nd4jTensor(t)
    val Nd4jt2 = new Nd4jTensor(dense)
    val filteredNd4jt2 = Nd4jt2 <= 241.0
    assert(Nd4jt1 == filteredNd4jt2)
  }

  test("greaterThanMask") {
    val dense = Nd4j.create(Array[Double](1, 241, 241, 242), Array(2, 2))
    val t = Nd4j.create(Array[Double](0, 0, 0, 242), Array(2, 2))
    val Nd4jt1 = new Nd4jTensor(t)
    val Nd4jt2 = new Nd4jTensor(dense)
    val filteredNd4jt2 = Nd4jt2 > 241.0
    assert(Nd4jt1 == filteredNd4jt2)
  }

  test("greaterThanOrEqualToMask") {
    val dense = Nd4j.create(Array[Double](1, 241, 241, 242), Array(2, 2))
    val t = Nd4j.create(Array[Double](0, 241, 241, 242), Array(2, 2))
    val Nd4jt1 = new Nd4jTensor(t)
    val Nd4jt2 = new Nd4jTensor(dense)
    val filteredNd4jt2 = Nd4jt2 >= 241.0
    assert(Nd4jt1 == filteredNd4jt2)
  }

  test("equalsMask") {
    val dense = Nd4j.create(Array[Double](1, 241, 241, 242), Array(2, 2))
    val t = Nd4j.create(Array[Double](0, 241, 241, 0), Array(2, 2))
    val Nd4jt1 = new Nd4jTensor(t)
    val Nd4jt2 = new Nd4jTensor(dense)
    val filteredNd4jt2 = Nd4jt2 := 241.0
    assert(Nd4jt1 == filteredNd4jt2)
  }

  test("notEqualsMask") {
    val dense = Nd4j.create(Array[Double](1, 241, 241, 242), Array(2, 2))
    val t = Nd4j.create(Array[Double](1, 0, 0, 242), Array(2, 2))
    val Nd4jt1 = new Nd4jTensor(t)
    val Nd4jt2 = new Nd4jTensor(dense)
    val filteredNd4jt2 = Nd4jt2 != 241.0
    assert(Nd4jt1 == filteredNd4jt2)
  }

  test("mask") {
    logger.info("In mask test ...")
    val dense = Nd4j.create(Array[Double](1, 241, 500, 1), Array(2, 2))
    val t = Nd4j.create(Array[Double](1, 100, 500, 1), Array(2, 2))
    val Nd4jt1 = new Nd4jTensor(t)
    val Nd4jt2 = new Nd4jTensor(dense)
    val filteredNd4jt2 = Nd4jt2.mask(p => p < 241.0 || p == 500, 100)
    assert(Nd4jt1 == filteredNd4jt2)
  }

  test("setMask") {
    logger.info("In setMask test ...")
    val dense = Nd4j.create(Array[Double](1, 241, 241, 1), Array(2, 2))
    val t = Nd4j.create(Array[Double](1, 100, 100, 1), Array(2, 2))
    val Nd4jt1 = new Nd4jTensor(t)
    val Nd4jt2 = new Nd4jTensor(dense)
    val filteredNd4jt2 = Nd4jt2.setMask(100) < 241.0
    assert(Nd4jt1 == filteredNd4jt2)
  }


  /**
   *
   * End Test relational operators
   */

  /**
   *
   * Test slicing
   */
  test("Nd4sSlice") {
    logger.info("In Nd4sSlice test ...")
    val nd = new Nd4jTensor((0d to 8d by 1d).toArray, Array(4, 2))
    logger.info(nd.toString)
    logger.info("slicing")
    logger.info(nd((0, 1)).toString)
    assert(true)
  }

  test("broadcastmatrixSubtraction") {
    logger.info("In broadcastmatrixSubtraction")
    val array = randVar
    val flattened = array.flatten
    val cascadedArray = flattened ++ flattened ++ flattened
    val square = Nd4j.create(array)

    val squareTensor = new Nd4jTensor(square)
    val cubeTensor = new Nd4jTensor((cascadedArray, Array(3) ++ squareTensor.shape))
    val cubeTensorShape = cubeTensor.shape
    val zeroTensor = cubeTensor.zeros(cubeTensorShape: _*)
    val broadcastSquareTensor = squareTensor.broadcast(Array(3, 6, 5))
    val subtractTensor = cubeTensor - broadcastSquareTensor
    assert(subtractTensor == zeroTensor)
  }



  test("detrend") {
    val axis = 0
    val sample = Array(1, 2, 4, 6, 54, 333, 2, 12, 4, 5, 7, 8, 3, 4, 2, 23, 45, 32,
                       33, 879, 34, 22, 34, 54, 55, 66, 23).map(p => p.toDouble)
    // NOTE : The solution matrix was obtaied by using the signal.detrend function from Scipy
    val solution = Array(4, 144.5, 3.67, 3.67, 13.33, 63.83, 1.83, -2.00, -6.17, -8.00, -289.00, -7.33, -7.33, -26.67,
      -127.67, -3.67, 4.00, 12.33, 4.00, 144.50, 3.67, 3.67, 13.33, 63.83, 1.83, -2.00, -6.17)
    val solutionDetrended = Nd4j.create(solution, Array(3, 3, 3))
    val cube = Nd4j.create(sample, Array(3, 3, 3))
    val cubeTensor = new Nd4jTensor(cube)
    val solutionTensor = new Nd4jTensor(solutionDetrended)
    val detrended = cubeTensor.detrend(0)
    assert(detrended == solutionTensor)
  }

  test("assign") {
    val sample = (1d to 27d by 1d).toArray
    val solution = sample.map(p => if (p < 10) 3 else p)
    val cube = Nd4j.create(sample, Array(3, 3, 3))
    val cubeTensor = new Nd4jTensor(cube)
    val solutionCube = Nd4j.create(solution, Array(3, 3, 3))
    val solutionTensor = new Nd4jTensor(solutionCube)

    val slice_1 = cubeTensor.slice((0, 1))
    slice_1.assign(new Nd4jTensor(Array(3, 3, 3, 3, 3, 3, 3, 3, 3), Array(3, 3)))

    assert(cubeTensor == solutionTensor)
  }

  test("std") {
    val sample = (0d to 27d by 1d).toArray
    val solution = (0d to 8d by 1d).map(p => 9.0).toArray
    val cube = Nd4j.create(sample, Array(3, 3, 3))
    val solCube = Nd4j.create(solution, Array(3, 3))
    val cubeTensor = new Nd4jTensor(cube)
    val solutionTensor = new Nd4jTensor(solCube)
    val std = cubeTensor.std(0)
    assert(solutionTensor == std)
  }

  test("skew") {
    val sample = (0d to 27d by 1d).toArray
    val cube = Nd4j.create(sample, Array(3, 3, 3))
    val cubeTensor = new Nd4jTensor(cube)
    val skw = cubeTensor.skew(0)
    val zeroSkew = new Nd4jTensor(Nd4j.zeros(3, 3))
    assert(skw == zeroSkew)
  }

  test("applySingleIndex") {
    val sample = (0d to 27d by 1d).toArray
    val squareSample = (0d to 8d by 1d).toArray
    val cube = Nd4j.create(sample, Array(3, 3, 3))
    val cubeTensor = new Nd4jTensor(cube)
    val square = Nd4j.create(squareSample, Array(3, 3))
    val squareTensor = new Nd4jTensor(square)
    val slicedSquare = cubeTensor(0)

    val zeroSkew = new Nd4jTensor(Nd4j.zeros(3, 3))
    assert(squareTensor == slicedSquare)
  }

}
