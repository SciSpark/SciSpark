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

import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.inverse
import org.nd4s.Implicits._
import scala.language.implicitConversions

/**
 * Wrapper around Nd4j INDArray.
 */
class Nd4jTensor(val tensor: INDArray) extends AbstractTensor {
  override type T = Nd4jTensor
  val name: String = "nd4j"
  val shape = tensor.shape.filter(p => p != 0 && p != 1)
  var mask = 0.0
  def this(shapePair: (Array[Double], Array[Int])) {
    this(Nd4j.create(shapePair._1, shapePair._2))
  }

  def this(loadFunc: () => (Array[Double], Array[Int])) {
    this(loadFunc())
  }

  def reshape(shape: Array[Int]) = new Nd4jTensor(Nd4j.create(this.data, shape))

  def broadcast(shape: Array[Int]) = {
    //new Nd4jTensor(tensor.broadcast(shape: _*))
    val extraDims = shape diff this.shape
    val totalExtraCopies = extraDims.reduce((A, B) => A * B)
    var rawLinearArray = this.data
    for (i <- 0 to totalExtraCopies by 1){
      rawLinearArray = rawLinearArray ++ rawLinearArray
    }
    new Nd4jTensor((rawLinearArray, shape))
  }

  def zeros(shape: Int*) = new Nd4jTensor(Nd4j.create(shape: _*))

  def map(f: Double => Double) = new Nd4jTensor(tensor.map(p => f(p)))

  def put(value: Double, shape: Int*) = tensor.putScalar(shape.toArray, value)

  def +(array: AbstractTensor) = new Nd4jTensor(tensor.addi(array.tensor))
  def +(scalar: Double) = new Nd4jTensor(tensor.addi(scalar))

  def -(array: AbstractTensor) = new Nd4jTensor(tensor.subi(array.tensor))
  def -(scalar: Double) = new Nd4jTensor(tensor.subi(scalar))

  def /(array: AbstractTensor) = new Nd4jTensor(tensor.divi(array.tensor))
  def /(scalar: Double) = new Nd4jTensor(tensor.divi(scalar))

  def *(array: AbstractTensor) = new Nd4jTensor(tensor.muli(array.tensor))
  def *(scalar: Double) = new Nd4jTensor(tensor.muli(scalar))

  def :+(array: AbstractTensor) = new Nd4jTensor(tensor.add(array.tensor))
  def :+(scalar: Double) = new Nd4jTensor(tensor.add(scalar))

  def :-(array: AbstractTensor) = new Nd4jTensor(tensor.sub(array.tensor))
  def :-(scalar: Double) = new Nd4jTensor(tensor.sub(scalar))

  def :/(array: AbstractTensor) = new Nd4jTensor(tensor.div(array.tensor))
  def :/(scalar: Double) = new Nd4jTensor(tensor.div(scalar))

  def :*(array: AbstractTensor) = new Nd4jTensor(tensor.mul(array.tensor))
  def :*(scalar: Double) = new Nd4jTensor(tensor.mul(scalar))

  /**
   * Masking operations
   */
  def mask(f: Double => Boolean, mask: Double = 0.0) = new Nd4jTensor(tensor.map(v => if (f(v)) v else mask))

  def setMask(num: Double): Nd4jTensor = {
    this.mask = num
    this
  }

  def <(num: Double): Nd4jTensor = new Nd4jTensor(tensor.map(p => if (p < num) p else mask))

  def >(num: Double): Nd4jTensor = new Nd4jTensor(tensor.map(p => if (p > num) p else mask))

  def <=(num: Double): Nd4jTensor = new Nd4jTensor(tensor.map(p => if (p <= num) p else mask))

  def >=(num: Double): Nd4jTensor = new Nd4jTensor(tensor.map(p => if (p >= num) p else mask))

  def :=(num: Double): Nd4jTensor = new Nd4jTensor(tensor.map(p => if (p == num) p else mask))

  def !=(num: Double): Nd4jTensor = new Nd4jTensor(tensor.map(p => if (p != num) p else mask))


  /**
   * Linear Algebra Operations
   */
  
  def **(array: AbstractTensor) = new Nd4jTensor(tensor.dot(array.tensor))
  
  def div(num: Double): Nd4jTensor = new Nd4jTensor(tensor.div(num))

  /**
   * SliceableArray operations
   */

  def rows = tensor.rows
  
  def cols = tensor.columns

  def apply = this

  def apply(ranges: (Int, Int)*) = {
    val rangeMap = ranges.map(p => TupleRange(p))
    val IndArray = tensor(rangeMap: _*)
    new Nd4jTensor(IndArray)
  }

  def apply(indexes: Int*) = tensor.get(indexes.toArray)

  def data : Array[Double] = tensor.data.asDouble()

  /**
   * Utility Functions
   */
  
  def cumsum = tensor.sumNumber.asInstanceOf[Double]

  def mean(axis : Int*) = new Nd4jTensor(tensor.mean(axis: _*))

  def detrend(axis: Int) = {
    val cube = tensor
    val dshape = cube.shape
    val N = dshape(axis)
    val bp = Array(0, N)
    val Nreg = bp.length - 1
    val rank = dshape.length
    val newdims = Array(axis) ++ (0 until axis) ++  ((axis + 1) until rank)
    val prod = dshape.reduce((A, B) => A * B)
    // The complete scipy conversion would be cube.transpose(newdims)).reshape(N, prod/N)
    // however, we cannot transpose and permute by the axis yet. The backend libraries
    // do not support it
    val newdata = cube.reshape(N, prod / N)
    for(m <- 0 until Nreg) {
      val Npts = bp(m + 1) - bp(m)
      val A = Nd4j.ones(Npts, 2)
      val normalizedRange = Nd4j.create((1 until Npts + 1).map(p => p * 1.0 / Npts).toArray)
      val Acol = A.getColumn(0)
      Acol.assign(normalizedRange)
      val sl = bp(m) -> bp(m + 1)
      // The least squares algorithm takes  and newdata[sl]
      // linalg.lstsq(A, newdaa[sl]
      //val newdata_sl = newdata(sl).dup

      // To compute the coefficient matrix
      // I relied on the following python algorithm found here:
      // https://en.wikipedia.org/wiki/Linear_least_squares_(mathematics)
      // Furthermore scipy's detrend algorithm only requires the coefficient matrix
      // So the algorithm wasn't needed
      // however it would be nice to use the lapack gelsd operator
      // The operation isn't yet suppored by nd4j
      val coef = inverse.InvertMatrix.invert(A.transpose().dot(A), true).dot(A.transpose()).dot(newdata(sl))
      val solution = Nd4j.create(N, prod/N, coef.ordering())
      val dot = A.mmuli(coef, solution)

      newdata(sl).subi(dot)
    }
    val tdshape = newdims.map(p => dshape(p))

    val ret = Nd4j.create(newdata.data().asDouble(), tdshape)
    new Nd4jTensor(ret)
  }

  override def toString = tensor.toString

  def isZero = tensor.mul(tensor).sumNumber.asInstanceOf[Double] <= 1E-9
  
  def isZeroShortcut = tensor.sumNumber().asInstanceOf[Double] <= 1E-9

  def max = tensor.maxNumber.asInstanceOf[Double]

  def min = tensor.minNumber.asInstanceOf[Double]

  private implicit def AbstractConvert(array: AbstractTensor): Nd4jTensor = array.asInstanceOf[Nd4jTensor]
  
}
