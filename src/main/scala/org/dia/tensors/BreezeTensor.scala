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

import breeze.linalg.{ DenseMatrix, sum }
import scala.language.implicitConversions

/**
 * Functions needed to perform operations with Breeze
 * We map every dimension to an index: dimension 1 -> Int 1, dimension 2 -> Int 2 etc.
 * Note that the DenseMatrix in Breeze by default uses Fortran column major ordering.
 * The constructor takes the transpose of this matrix to follow row major ordering.
 */
class BreezeTensor(val tensor: DenseMatrix[Double]) extends AbstractTensor {
  override type T = BreezeTensor
  val name: String = "breeze"
  val shape = Array(tensor.rows, tensor.cols)

  /**
   * Constructs a BreezeTensor from a linear data array and the shape array.
   * Note that this calls the transpose constructor of the DenseMatrix, so it follows
   * row major ordering.
   *
   * @param shapePair A pair of arrays consisting of an Array[Double] and Array[Int]
   */
  def this(shapePair: (Array[Double], Array[Int])) {
    this(new DenseMatrix[Double](shapePair._2(0), shapePair._2(1), shapePair._1, 0, shapePair._2(1), true))
  }

  def this(loadFunc: () => (Array[Double], Array[Int])) {
    this(loadFunc())
  }

  /**
   * Constructs a zeroed BreezeTensor.
   *
   * @param shape shape the BreezeTensor of zeros should have.
   */
  def zeros(shape: Int*) = {
    val array = (0d to (shape(0) * shape(1)) by 1d).map(p => 0.0).toArray
    new DenseMatrix[Double](shape(0), shape(1), array, 0, shape(1), true)
  }

  def map(f: Double => Double) = tensor.map(f)

  def put(value: Double, shape: Int*) = tensor.update(shape(0), shape(1), value)

  def +(array: AbstractTensor) = tensor + array.tensor

  def -(array: AbstractTensor) = tensor - array.tensor

  def \(array: AbstractTensor) = tensor \ array.tensor

  def /(array: AbstractTensor) = tensor / array.tensor

  def *(array: AbstractTensor) = tensor :* array.tensor

  def <=(num: Double) = tensor.map(v => if (v <= num) v else 0.0)

  def :=(num: Double) = tensor.map(v => if (v == num) v else 0.0)

  /**
   * Linear Algebra Operations
   */

  def **(array: AbstractTensor) = tensor * array.tensor

  def data = tensor.t.toArray

  /**
   * SlicableArray operations
   */

  def rows = tensor.rows
  
  def cols = tensor.cols

  def apply = this

  def apply(ranges: (Int, Int)*): BreezeTensor = {
    tensor(ranges(0)._1 to (ranges(0)._2 - 1), ranges(1)._1 to (ranges(1)._2 - 1))
  }

  def apply(indexes: Int*) = {
    tensor(indexes(0), indexes(1))
  }

  /**
   * Utility Operations
   */
  
  def cumsum = sum(tensor)

  def isZero = sum(tensor :* tensor) <= 1E-9
  
  def isZeroShortcut = sum(tensor) <= 1E-9

  override def toString: String = if (tensor != null) tensor.toString() else null

  def max = breeze.linalg.max(tensor)

  def min = breeze.linalg.min(tensor)

  /**
   * Due to implicit conversions we can do operations on BreezeTensors and DenseMatrix
   */
  
  private implicit def convert(array: DenseMatrix[Double]): BreezeTensor = new BreezeTensor(array)

  private implicit def abstractConvert(brzT: AbstractTensor): BreezeTensor = brzT.asInstanceOf[BreezeTensor]

}

