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
import org.nd4s.Implicits._
import scala.language.implicitConversions

/**
 * Wrapper around Nd4j INDArray.
 */
class Nd4jTensor(val tensor: INDArray) extends AbstractTensor {
  override type T = Nd4jTensor
  val name: String = "nd4j"
  val shape = tensor.shape

  def this(shapePair: (Array[Double], Array[Int])) {
    this(Nd4j.create(shapePair._1, shapePair._2))
  }

  def this(loadFunc: () => (Array[Double], Array[Int])) {
    this(loadFunc())
  }

  def zeros(shape: Int*) = new Nd4jTensor(Nd4j.create(shape: _*))

  def map(f: Double => Double) = new Nd4jTensor(tensor.map(p => f(p)))

  def put(value: Double, shape: Int*) = tensor.putScalar(shape.toArray, value)

  def +(array: AbstractTensor) = new Nd4jTensor(tensor.add(array.tensor))

  def -(array: AbstractTensor) = new Nd4jTensor(tensor.sub(array.tensor))

  def \(array: AbstractTensor) = new Nd4jTensor(tensor.div(array.tensor))

  def /(array: AbstractTensor) = new Nd4jTensor(tensor.div(array.tensor))

  def *(array: AbstractTensor) = new Nd4jTensor(tensor.mul(array.tensor))

  /**
   * Masking operations
   */

  def <=(num: Double): Nd4jTensor = new Nd4jTensor(tensor.map(p => if (p < num) p else 0.0))

  def :=(num: Double) = new Nd4jTensor(tensor.map(p => if (p == num) p else 0.0))

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

  def data = tensor.data.asDouble()

  /**
   * Utility Functions
   */
  
  def cumsum = tensor.sumNumber.asInstanceOf[Double]

  override def toString = tensor.toString

  def isZero = tensor.mul(tensor).sumNumber.asInstanceOf[Double] <= 1E-9
  
  def isZeroShortcut = tensor.sumNumber().asInstanceOf[Double] <= 1E-9

  def max = tensor.maxNumber.asInstanceOf[Double]

  def min = tensor.minNumber.asInstanceOf[Double]

  private implicit def AbstractConvert(array: AbstractTensor): Nd4jTensor = array.asInstanceOf[Nd4jTensor]
  
}
