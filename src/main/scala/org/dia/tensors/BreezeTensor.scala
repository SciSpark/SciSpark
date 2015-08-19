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

import breeze.linalg.{DenseMatrix, sum}

import scala.language.implicitConversions

/**
 * Functions needed to perform operations with Breeze
 * We map every dimension to an index ex : dimension 1 -> Int 1, dimension 2 -> Int 2 etc.
 * Note that the DenseMatrix in breeze by default uses fortran column major ordering.
 * The constructor takes the transpose of this matrix to follow row major ordering.
 */
class BreezeTensor(val tensor: DenseMatrix[Double]) extends AbstractTensor {
  override type T = BreezeTensor
  val name: String = "breeze"
  val shape = Array(tensor.rows, tensor.cols)

  /**
   * Takes the linear array and the shape array.
   * Note that this calls the transpose constructor of the DenseMatrix, so it follows
   * row major ordering.
   * @param shapePair a pair of arrays consisting of an Array[Double] and Array[Int]
   */
  def this(shapePair: (Array[Double], Array[Int])) {
    this(new DenseMatrix[Double](shapePair._2(0), shapePair._2(1), shapePair._1, 0, shapePair._2(1), true))
  }

  def this(loadFunc: () => (Array[Double], Array[Int])) {
    this(loadFunc())
  }


  def zeros(shape: Int*): BreezeTensor = DenseMatrix.zeros[Double](shape(0), shape(1))

  def map(f : Double => Double) : BreezeTensor = tensor.map(p => f(p))
  def put(value: Double, shape: Int*): Unit = tensor.update(shape(0), shape(1), value)

  def +(array: BreezeTensor): BreezeTensor = tensor + array.tensor

  def -(array: BreezeTensor): BreezeTensor = tensor - array.tensor

  def \(array: BreezeTensor): BreezeTensor = tensor \ array.tensor

  def /(array: BreezeTensor): BreezeTensor = tensor / array.tensor

  def *(array: AbstractTensor): BreezeTensor = tensor :* array.tensor

  def <=(num: Double): BreezeTensor = tensor.map(v => if (v <= num) v else 0.0)

  def :=(num : Double): BreezeTensor = tensor.map(v => if (v == num) v else 0.0)

  /**
   * Linear Algebra Operations
   */
  def **(array: BreezeTensor): BreezeTensor = tensor * array.tensor

  def data: Array[Double] = tensor.t.toArray

  /**
   * SlicableArray operations
   */

  def cols: Int = tensor.cols

  def rows: Int = tensor.rows

  def apply: BreezeTensor = this

  def apply(ranges: (Int, Int)*): BreezeTensor = {
    tensor(ranges(0)._1 to (ranges(0)._2 - 1), ranges(1)._1 to (ranges(1)._2 - 1))
  }

  def apply(indexes: Int*): Double = {
    tensor(indexes(0), indexes(1))
  }

  /**
   * Utility Operations
   */
  def cumsum: Double = sum(tensor)

  def isZero: Boolean = sum(tensor) <= 1E-9

  override def toString: String = if (tensor != null) tensor.toString() else null

  def max : Double = tensor.max

  def min : Double = tensor.min

  /**
   * Due to implicit conversions we can do operations on BreezeTensors and DenseMatrix
   */
  private implicit def convert(array: DenseMatrix[Double]): BreezeTensor = new BreezeTensor(array)

  private implicit def abstractConvert(brzT: AbstractTensor): BreezeTensor = brzT.asInstanceOf[BreezeTensor]
}

