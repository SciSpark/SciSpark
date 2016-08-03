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

import scala.language.implicitConversions

import breeze.linalg.{sum, DenseMatrix}

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
  var maskVal = 0.0

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

  def reshape(shape: Array[Int]): BreezeTensor = {
    new BreezeTensor((this.data, shape))
  }

  def broadcast(shape: Array[Int]): BreezeTensor = {
    throw new Exception("BreezeTensor does not support the broadcast operation yet")
  }

  /**
   * Constructs a zeroed BreezeTensor.
   *
   * @param shape shape the BreezeTensor of zeros should have.
   */
  def zeros(shape: Int*): BreezeTensor = {
    val array = (0d to (shape(0) * shape(1)) by 1d).map(p => 0.0).toArray
    new DenseMatrix[Double](shape(0), shape(1), array, 0, shape(1), true)
  }

  def map(f: Double => Double): BreezeTensor = tensor.map(f)

  def put(value: Double, shape: Int*): Unit = tensor.update(shape(0), shape(1), value)

  def +(array: AbstractTensor): BreezeTensor = tensor :+= array.tensor

  def +(scalar: Double): BreezeTensor = tensor :+= scalar

  def -(array: AbstractTensor): BreezeTensor = tensor :-= array.tensor

  def -(scalar: Double): BreezeTensor = tensor :-= scalar

  def /(array: AbstractTensor): BreezeTensor = tensor :/= array.tensor

  def /(scalar: Double): BreezeTensor = tensor :/= scalar

  def *(array: AbstractTensor): BreezeTensor = tensor *= array.tensor

  def *(scalar: Double): BreezeTensor = tensor :*= scalar

  def :+(array: AbstractTensor): BreezeTensor = tensor + array.tensor

  def :+(scalar: Double): BreezeTensor = tensor + scalar

  def :-(array: AbstractTensor): BreezeTensor = tensor - array.tensor

  def :-(scalar: Double): BreezeTensor = tensor - scalar

  def :/(array: AbstractTensor): BreezeTensor = tensor / array.tensor

  def :/(scalar: Double): BreezeTensor = tensor / scalar

  def :*(array: AbstractTensor): BreezeTensor = tensor :* array.tensor

  def :*(scalar: Double): BreezeTensor = tensor :* scalar

  /**
   * Masking operations
   */
  def mask(f: Double => Boolean, mask: Double = 0.0): BreezeTensor = tensor.map(v => if (f(v)) v else mask)

  def setMask(num: Double): BreezeTensor = {
    this.maskVal = num
    this
  }

  def <(num: Double): BreezeTensor = tensor.map(v => if (v < num) v else maskVal)

  def >(num: Double): BreezeTensor = tensor.map(v => if (v > num) v else maskVal)

  def <=(num: Double): BreezeTensor = tensor.map(v => if (v <= num) v else maskVal)

  def >=(num: Double): BreezeTensor = tensor.map(v => if (v >= num) v else maskVal)

  def :=(num: Double): BreezeTensor = tensor.map(v => if (v == num) v else maskVal)

  def !=(num: Double): BreezeTensor = tensor.map(v => if (v != num) v else maskVal)

  /**
   * Linear Algebra Operations
   */
  def **(array: AbstractTensor): BreezeTensor = tensor * array.tensor

  def div(num: Double): BreezeTensor = tensor / num

  def data: Array[Double] = tensor.t.toArray

  /**
   * SlicableArray operations
   */

  def rows: Int = tensor.rows

  def cols: Int = tensor.cols

  def apply: BreezeTensor = this

  def apply(ranges: (Int, Int)*): BreezeTensor = {
    tensor(ranges(0)._1 until ranges(0)._2, ranges(1)._1 until ranges(1)._2)
  }

  def apply(indexes: Int*): Double = {
    tensor(indexes(0), indexes(1))
  }

  /**
   * Utility Operations
   */

  def cumsum: Double = sum(tensor)

  /**
   * TODO :: Implement the mean along axis function for BreezeTensor
   *
   * @param axis dimension to take mean along
   * @return
   */
  def mean(axis: Int*): BreezeTensor = {
    throw new Exception("BreezeTensor does not support the mean along axis operation yet")
  }

  /**
   * TODO :: Implement detrend along axis
   *
   * @param axis dimension to detrend along
   * @return
   */
  def detrend(axis: Int): BreezeTensor = {
    throw new Exception("BreezeTensor does not yet support detrending")
  }

  def std(axis: Int*): BreezeTensor = {
    throw new Exception("BreezeTensor does not yet support standard deviation along axis")
  }

  def skew(axis: Int*): BreezeTensor = {
    throw new Exception("BreezeTensor does not yet support skewness along an axis")
  }

  /**
   * Copies over the data in a new tensor to the current tensor
   *
   * @param newTensor tensor to assign values from
   * @return
   */
  def assign(newTensor: AbstractTensor): BreezeTensor = {
    this.tensor := newTensor.tensor
  }

  def isZero: Boolean = sum(tensor :* tensor) <= 1E-9

  def isZeroShortcut: Boolean = sum(tensor) <= 1E-9

  override def toString: String = if (tensor != null) tensor.toString() else null

  def max: Double = breeze.linalg.max(tensor)

  def min: Double = breeze.linalg.min(tensor)

  def copy: BreezeTensor = tensor.copy

  /**
   * Due to implicit conversions we can do operations on BreezeTensors and DenseMatrix
   */

  private implicit def convert(array: DenseMatrix[Double]): BreezeTensor = new BreezeTensor(array)

  private implicit def abstractConvert(brzT: AbstractTensor): BreezeTensor = brzT.asInstanceOf[BreezeTensor]

}

