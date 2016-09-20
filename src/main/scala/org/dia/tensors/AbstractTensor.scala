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

import java.util

/**
 * An abstract tensor
 */
trait AbstractTensor extends Serializable with SliceableArray {

  type T <: AbstractTensor
  val name: String
  val BACKGROUND = 0.0
  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def reshape(shape: Array[Int]): T
  def broadcast(shape: Array[Int]): T
  def zeros(shape: Int*): T

  def map(f: Double => Double): AbstractTensor

  /**
   * Indexed Operations
   */

  def put(value: Double, shape: Int*): Unit

  /**
   * Element-wise Operations
   */

  def +(array: AbstractTensor): T
  def +(scalar: Double): T

  def -(array: AbstractTensor): T
  def -(scalar: Double): T

  def *(array: AbstractTensor): T
  def *(scalar: Double): T

  def /(array: AbstractTensor): T
  def /(scalar: Double): T

  def +=(array: AbstractTensor): T
  def +=(scalar: Double): T

  def -=(array: AbstractTensor): T
  def -=(scalar: Double): T

  def *=(array: AbstractTensor): T
  def *=(scalar: Double): T

  def /=(array: AbstractTensor): T
  def /=(scalar: Double): T

  /**
   * Linear Algebra Operations
   */

  def **(array: AbstractTensor): T

  def div(num: Double): T

  /**
   * Masking operations
   */
  def mask(f: Double => Boolean, mask: Double) : T
  def setMask(num: Double): T
  def <(num: Double): T
  def >(num: Double): T
  def <=(num: Double): T
  def >=(num: Double): T
  def :=(num: Double): T
  def !=(num: Double): T

  /**
   * Returns the data as a flattened array
   *
   */
  def data: Array[Double]

  /**
   * Stacks the current array ontop of the input array
   * in row ordered fashion
   * [this.array, array]
   *
   * The resulting array will have shape (2, originalShape)
   *
   * @param array the new array to stack on
   * @return the stacked array
   */
  def stack(array : AbstractTensor*) : T

  /**
   * Returns the data dimensions
   *
   */
  def shape: Array[Int]

  /**
   * Utility Methods
   */

  def cumsum: Double
  def mean(axis : Int*) : T
  def detrend(axis: Int) : T
  def std(axis: Int*): T
  def skew(axis: Int*): T
  def assign(newTensor: AbstractTensor) : T
  def toString: String

  /**
   * Reduces the resolution of the tensor into square blocks
   * by taking the average.
   *
   * @param blockSize block size used for aggregation
   * @param invalid value used to signify invalid value
   * @return aggregated tensor obtained by taking the average of
   * values within blocks ignoring invalid values
   */
  def reduceResolution(blockSize: Int, invalid: Double = Double.NaN): AbstractTensor = {
    val largeArray = this.asInstanceOf[AbstractTensor]
    val numRows = largeArray.rows
    val numCols = largeArray.cols
    val reducedMatrix = zeros(numRows / blockSize, numCols / blockSize)

    for (row <- 0 until reducedMatrix.rows) {
      for (col <- 0 until reducedMatrix.cols) {
        val rowRange = (row * blockSize) -> ((row + 1) * blockSize)
        val columnRange = (col * blockSize) -> ((col + 1) * blockSize)
        val block = this(rowRange, columnRange).copy
        val numValid = block.data.count(_ != invalid)
        val avg = if (numValid > 0) block.cumsum / numValid else 0.0
        reducedMatrix.put(avg, row, col)
      }
    }
    reducedMatrix
  }

  /**
   * Reduces the resolution of the tensor into rectangle blocks
   * by taking the average.
   *
   * Similar to above method reduceResolution.
   */
  def reduceRectangleResolution(
      rowSize: Int,
      colSize: Int,
      invalid: Double = Double.NaN): AbstractTensor = {

    val largeArray = this.asInstanceOf[AbstractTensor]
    val numRows = largeArray.rows()
    val numCols = largeArray.cols()
    val reducedMatrix = zeros(numRows / rowSize, numCols / colSize)

    for (row <- 0 until reducedMatrix.rows) {
      for (col <- 0 until reducedMatrix.cols) {
        val rowRange = (row * rowSize) -> ((row + 1) * rowSize)
        val columnRange = (col * colSize) -> ((col + 1) * colSize)
        val block = this(rowRange, columnRange).copy
        val numValid = block.data.count(_ != invalid)
        val avg = if (numValid > 0) block.cumsum / numValid else 0.0
        reducedMatrix.put(avg, row, col)
      }
    }
    reducedMatrix
  }

  /**
   * Computes connected component labels of tensor.
   *
   * Note that for garbage collection purposes we use one ArrayStack.
   * We push in row/col tuples two ints at a time, and pop two ints at a time.
   * Also note that the components are labeled starting at 1.
   *
   * This is just doing a depth-first-search (DFS) over the tensor cells.
   * (The tensor cells are a graph with two cells being connected iff
   * they are next to each other horizontally or vertically (but not diagonally).)
   *
   * @return a single tensor with the associated component numbers,
   * together with the total number of components.
   */
  def labelComponents(): (AbstractTensor, Int) = {
    val fourVector = List((1, 0), (-1, 0), (0, 1), (0, -1))
    val rows = this.shape(0)//tensor.rows()
    val cols = this.shape(1) //tensor.cols()
    val labels = zeros(this.shape: _*)
    var label = 1
    val stack = new util.ArrayDeque[Int](this.rows + this.cols * 10)

    /**
     * Whether we already assigned a component label to the coordinate.
     *
     * (row,col) is already labeled if
     * (i) it's out of bounds OR
     * (ii) its value is BACKGROUND (=0) OR
     * (iii) it has already been assigned to a component,
     * i.e. labels(row,col) is not BACKGROUND (=0)
     *
     * @param row the row to check
     * @param col the column to check
     * @return whether (row,col) is already labeled
     */
    def isLabeled(row: Int, col: Int): Boolean = {
      if (row < 0 || col < 0 || row >= rows || col >= cols) return true
      this(row, col) == BACKGROUND || labels(row, col) != BACKGROUND
    }

    /**
     * Pushes unlabeled neighbors onto the stack.
     *
     * @param currentLabel the component label being added
     */
    def dfs(currentLabel: Int): Unit = {
      while (!stack.isEmpty) {
        /**
         *  Note that when popping, we pop col, then row.
         *  When pushing, we push row, then col.
         */
        val col = stack.pop
        val row = stack.pop
        labels.put(currentLabel, row, col)
        val neighbors = fourVector.map(p => (p._1 + row, p._2 + col))
        for (neighbor <- neighbors) {
          if (!isLabeled(neighbor._1, neighbor._2)) {
            val row = neighbor._1
            val col = neighbor._2
            stack.push(row)
            stack.push(col)
          }
        }
      }
    }

    /** Main loop */
    for (row <- 0 until rows) {
      for (col <- 0 until cols) {
        if (!isLabeled(row, col)) {
          stack.push(row)
          stack.push(col)
          dfs(label)
          label += 1
        }
      }
    }
    (labels, label - 1)
  }

  /**
   * It will be called when checking equality against
   * different implementations of AbstractTensor.
   * Due to properties of Doubles, the equals method
   * utilizes the percent error rather than checking absolute equality.
   * The threshold for percent error is if it is greater than 0.5% or .005.
   * @param any the object to compare to
   * @return
   */
  override def equals(any: Any): Boolean = {
    val array = any.asInstanceOf[AbstractTensor]
    val shape = array.shape
    val thisShape = this.shape

    if(!shape.sameElements(thisShape)) return false

    val thisData = this.data
    val otherData = array.data
    for(index <- thisData.indices) {
      val left = thisData(index)
      val right = otherData(index)
      if(left != 0.0 && right == 0.0) {
        return false
      } else if (right == 0.0 && left != 0.0) {
        return false
      } else if ( right != 0.0 && left != 0.0) {
        val absoluteError = Math.abs(left - right)
        val percentageError = absoluteError / Math.max(left, right)
        if(percentageError > 5E-3) {
          return false
        }
      }

    }
    true
  }

  override def hashCode(): Int = super.hashCode()

  def copy: T

  def isZero: Boolean
  /**
   *  Shortcut test whether tensor is zero
   *  in case we know its entries are all non-negative.
   */
  def isZeroShortcut: Boolean
  def max: Double
  def min: Double

}
