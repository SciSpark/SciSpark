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
 */
class BreezeTensor(val tensor: DenseMatrix[Double]) extends AbstractTensor {
  override type T = BreezeTensor
  val name: String = "breeze"
  val shape = Array(tensor.rows, tensor.cols)

  def this(shapePair: (Array[Double], Array[Int])) {
    this(new DenseMatrix[Double](shapePair._2(0), shapePair._2(1), shapePair._1, 0, shapePair._2(1), true))
  }

  def this(loadFunc: () => (Array[Double], Array[Int])) {
    this(loadFunc())
  }

  /**
   * Reduces the resolution of a DenseMatrix
   * @param blockSize the size of n x n size of blocks.
   * @return
   */
  def reduceResolution(blockSize: Int): BreezeTensor = {
    val largeArray = tensor
    val numRows = largeArray.rows
    val numCols = largeArray.cols
    val reducedSize = numRows * numCols / (blockSize * blockSize)
    val reducedMatrix = DenseMatrix.zeros[Double](numRows / blockSize, numCols / blockSize)

    for (row <- 0 to (reducedMatrix.rows - 1)) {
      for (col <- 0 to (reducedMatrix.cols - 1)) {
        val rowIndices = (row * blockSize) to (((row + 1) * blockSize) - 1)
        val colIndices = (col * blockSize) to (((col + 1) * blockSize) - 1)
        val block = largeArray(rowIndices, colIndices)
        val totalsum = sum(block)
        val validCount = block.findAll(p => p != 0.0).size.toDouble
        val average = if (validCount > 0) totalsum / validCount else 0.0
        reducedMatrix(row to row, col to col) := average
        reducedMatrix
      }
    }
    new BreezeTensor(reducedMatrix)
  }

  /**
   * Due to implicit conversions we can do operations on BreezeTensors and DenseMatrix
   */
  private implicit def convert(array: DenseMatrix[Double]) = new BreezeTensor(array)

  def +(array: BreezeTensor): BreezeTensor = tensor + array.tensor

  def -(array: BreezeTensor): BreezeTensor = tensor - array.tensor

  def \(array: BreezeTensor): BreezeTensor = tensor \ array.tensor

  def /(array: BreezeTensor): BreezeTensor = tensor / array.tensor

  def *(array: BreezeTensor): BreezeTensor = tensor :* array.tensor

  def <=(num: Double): BreezeTensor = tensor.map(v => if (v <= num) v else 0.0)

  /**
   * Linear Algebra Operations
   */
  def **(array: BreezeTensor): BreezeTensor = tensor * array.tensor

  override def toString: String = if (tensor != null) tensor.toString else null

  implicit def apply: BreezeTensor = this

  implicit def apply(ranges: (Int, Int)*): BreezeTensor = tensor(ranges(0)._1 to (ranges(0)._2 - 1), ranges(1)._1 to (ranges(1)._2 - 1))

  override def equals(array: BreezeTensor): Boolean = tensor == array.tensor

  override def getUnderlying(): (Array[Double], Array[Int]) = (data, shape)

  def data: Array[Double] = tensor.t.toArray


}

