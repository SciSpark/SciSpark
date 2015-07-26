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

import org.nd4j.api.Implicits._
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.nd4j.linalg.indexing.BooleanIndexing
import org.nd4j.linalg.indexing.conditions.LessThanOrEqual
import org.nd4j.linalg.indexing.functions.{Identity, Zero}

import scala.language.implicitConversions

/**
 * The Nd4j Functional operations
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

  def reduceResolution(blockSize: Int): Nd4jTensor = {
    val largeArray = tensor
    val numRows = largeArray.rows()
    val numCols = largeArray.columns()
    val reducedSize = numRows * numCols / (blockSize * blockSize)

    val reducedMatrix = Nd4j.create(numRows / blockSize, numCols / blockSize)

    for (row <- 0 to (reducedMatrix.rows - 1)) {
      for (col <- 0 to (reducedMatrix.columns - 1)) {
        val rowRange = (row * blockSize) -> (((row + 1) * blockSize))
        val columnRange = (col * blockSize) -> (((col + 1) * blockSize))
        val block = tensor(rowRange, columnRange)
        val numNonZero = block.data.asDouble.filter(p => p != 0).size
        val avg = if (numNonZero > 0) (block.sum(Integer.MAX_VALUE).getDouble(0) / numNonZero) else 0.0
        reducedMatrix.put(row, col, avg)
      }
    }
    new Nd4jTensor(reducedMatrix)
  }

  def +(array: Nd4jTensor): Nd4jTensor = new Nd4jTensor(tensor + array.tensor)

  //implicit def convert(array : INDArray) : Nd4jTensor = new Nd4jTensor(array)

  def -(array: Nd4jTensor): Nd4jTensor = new Nd4jTensor(tensor - array.tensor)

  def \(array: Nd4jTensor): Nd4jTensor = new Nd4jTensor(tensor \ array.tensor)

  def /(array: Nd4jTensor): Nd4jTensor = new Nd4jTensor(tensor / array.tensor)

  def *(array: Nd4jTensor): Nd4jTensor = new Nd4jTensor(tensor * array.tensor)

  /**
   * Masking operations
   */

  def <=(num: Double): Nd4jTensor = {
    val tensorCopy = tensor
    BooleanIndexing.applyWhere(tensorCopy, new LessThanOrEqual(num), new Identity, new Zero)
    new Nd4jTensor(tensorCopy)
  }

  /**
   * Linear Algebra Operations
   */
  def **(array: Nd4jTensor): Nd4jTensor = new Nd4jTensor(tensor ** array.tensor)

  override def toString: String = tensor.toString

  implicit def apply: Nd4jTensor = this

  implicit def apply(ranges: (Int, Int)*): Nd4jTensor = {
    val rangeMap = ranges.map(p => TupleRange(p))
    val IndArray = tensor(rangeMap: _*)
    new Nd4jTensor(IndArray)
  }

  override def equals(array: Nd4jTensor): Boolean = tensor == array.tensor

  override def getUnderlying(): (Array[Double], Array[Int]) = (data, shape)

  def data: Array[Double] = tensor.data.asDouble()
}
