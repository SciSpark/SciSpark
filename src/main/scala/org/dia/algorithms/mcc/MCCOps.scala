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
package org.dia.algorithms.mcc

import java.util
import org.dia.core.SciTensor
import org.dia.tensors.AbstractTensor

/**
 * Utilities to compute connected components within tensor.
 */
object MCCOps {

  val BACKGROUND = 0.0

  /**
   * Reduces the resolution of the tensor into square blocks
   * by taking the average.
   *
   * @param tensor original tensor
   * @param blockSize block size used for aggregation
   * @param invalid value used to signify invalid value
   * @return aggregated tensor obtained by taking the average of
   * values within blocks ignoring invalid values
   */
  def reduceResolution(tensor: AbstractTensor, blockSize: Int, invalid: Double): AbstractTensor = {
    val largeArray = tensor
    val numRows = largeArray.rows
    val numCols = largeArray.cols
    val reducedMatrix = tensor.zeros(numRows / blockSize, numCols / blockSize)

    for (row <- 0 to (reducedMatrix.rows - 1)) {
      for (col <- 0 to (reducedMatrix.cols - 1)) {
        val rowRange = (row * blockSize) -> ((row + 1) * blockSize)
        val columnRange = (col * blockSize) -> ((col + 1) * blockSize)
        val block = tensor(rowRange, columnRange)
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
  def reduceRectangleResolution(tensor: AbstractTensor, rowblockSize: Int, columnblockSize: Int, invalid: Int): AbstractTensor = {
    val largeArray = tensor
    val numRows = largeArray.rows
    val numCols = largeArray.cols
    val reducedMatrix = tensor.zeros(numRows / rowblockSize, numCols / columnblockSize)

    for (row <- 0 to (reducedMatrix.rows - 1)) {
      for (col <- 0 to (reducedMatrix.cols - 1)) {
        val rowRange = (row * rowblockSize) -> ((row + 1) * rowblockSize)
        val columnRange = (col * columnblockSize) -> ((col + 1) * columnblockSize)
        val block = tensor(rowRange, columnRange)
        val numValid = block.data.count(_ != invalid)
        val avg = if (numValid > 0) block.cumsum / numValid else 0.0
        reducedMatrix.put(avg, row, col)
      }
    }
    reducedMatrix
  }

  /**
   * Computes the cloud connected components.
   *
   * @param tensor the tensor
   * @return List of tensors that are the connected components.
   * Returned as a mask but with metadata such as number of component,
   * area, difference (= max - min).
   */
  def findCloudComponents(tensor: SciTensor): List[SciTensor] = {
    val labeledTensors = findConnectedComponents(tensor.tensor)
    val absT: AbstractTensor = tensor.tensor

    val comps = labeledTensors.indices.map(p => {
      val masked: AbstractTensor = labeledTensors(p).map(a => if (a != 0.0) 1.0 else a)
      val areaTuple = computeBasicStats(masked * absT)
      val area = areaTuple._1
      val max = areaTuple._2
      val min = areaTuple._3
      val metadata = tensor.metaData += (("AREA", "" + area)) += (("CONVECTIVE_FRACTION", "" + (min/max))) += (("COMPONENT", "" + p))
      new SciTensor(tensor.varInUse, masked, metadata)
    })
    comps.toList
  }

  /**
   * Computes connected components of tensor.
   *
   * Note that each returned component is the entire tensor itself
   * just with the cells that do not fall into the component
   * zeroed out. The cells that do fall into the component all have
   * the number of the component as their value.
   *
   * @param tensor the tensor
   * @return masked connected components
   */
  def findConnectedComponents(tensor: AbstractTensor): List[AbstractTensor] = {
    val tuple = labelConnectedComponents(tensor)
    val labeled = tuple._1
    val maxVal = tuple._2
    val maskedLabels = (1 to maxVal).toArray.map(labeled := _.toDouble)
    maskedLabels.toList
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
   * @param tensor the tensor
   * @return a single tensor with the associated component numbers,
   * together with the total number of components.
   */
  def labelConnectedComponents(tensor: AbstractTensor): (AbstractTensor, Int) = {
    val fourVector = List((1, 0), (-1, 0), (0, 1), (0, -1))
    val rows = tensor.rows
    val cols = tensor.cols
    val labels = tensor.zeros(tensor.shape: _*)
    var label = 1
    val stack = new util.ArrayDeque[Int](tensor.rows + tensor.cols * 10)

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
      tensor(row, col) == BACKGROUND || labels(row, col) != BACKGROUND
    }

    /**
     * Pushes unlabeled neighbors onto the stack.
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
    for (row <- 0 to (rows - 1)) {
      for (col <- 0 to (cols - 1)) {
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
   * Computes basic statistics about tensor.
   *
   * @param tensor the tensor
   * @return area, min, max
   */
  def computeBasicStats(tensor: AbstractTensor): (Double, Double, Double) = {
    var count = 0.0
    var min = Double.MaxValue
    var max = Double.MinValue
    val masked = tensor.map(p => {
      if (p != 0) {
        if (p < min) min = p
        if (p > max) max = p
        count += 1.0
        1.0
      } else {
        p
      }
    })
    (count, max, min)
  }

  /**
   * Very similar to labelConnectedComponents except also
   * adds number of components as meta information.
   * Further it is expecting and returning a SciTensor,
   * not an AbstractTensor.
   */
  def findCloudElements(tensor: SciTensor): SciTensor = {
    val labeledTensor = labelConnectedComponents(tensor.tensor)
    val metadata = tensor.metaData += (("NUM_COMPONENTS", "" + labeledTensor._2))
    new SciTensor(tensor.varInUse, labeledTensor._1, metadata)
  }

  /**
   * Checks whether connected component is indeed a cloud.
   *
   * @param comp masked component with AREA and CONVECTIVE_FRACTION meta info
   * @todo make sure this is only applied if AREA and CONVECTIVE_FRACTION
   * meta fields exist!
   */
  def checkCriteria(comp: SciTensor): Boolean = {
    val hash = comp.metaData
    val area = hash("AREA").toDouble
    val convectiveFrac = hash("CONVECTIVE_FRACTION").toDouble
    ((area >= 2400.0) || ((area < 2400.0) && ((convectiveFrac) > 0.9)))
  }

  /**
   * Returns the edges from first to second tensor.
   *
   * @param sciTensor1 first tensor
   * @param sciTensor2 second tensor
   * @return list of (String, String) = (frameId1:componentId1, frameId2:componentId2)
   * such that componentId1+2 satisfy the criteria and overlap.
   */
  def checkComponentsOverlap(sciTensor1: SciTensor, sciTensor2: SciTensor): List[(String, String)] = {
    val currentTimeRDD = MCCOps.findCloudElements(sciTensor1)
    val nextTimeRDD = MCCOps.findCloudElements(sciTensor2)
    var edges = List.empty[(String, String)]
    /** Cartesian product */
    (1 to currentTimeRDD.metaData("NUM_COMPONENTS").toInt).foreach(cIdx => {
      (1 to nextTimeRDD.metaData("NUM_COMPONENTS").toInt).foreach(nIdx => {
        /** check if valid */
        if (checkCriteria(sciTensor1.tensor.data, currentTimeRDD.tensor.data, cIdx)
          && checkCriteria(sciTensor2.tensor.data, nextTimeRDD.tensor.data, nIdx)) {
          /** verify overlap */
          if (overlap(currentTimeRDD.tensor, nextTimeRDD.tensor, cIdx, nIdx)) {
            val tup = (currentTimeRDD.metaData("FRAME") + ":" + cIdx, nextTimeRDD.metaData("FRAME") + ":" + nIdx)
            edges :+= tup
          }
        }
      })
    })
    edges
  }

  /**
   * Whether component satisfies criteria.
   *
   * @param origData linearized tensor values
   * @param compData linearized tensor component labels
   * @param compNum component label
   * @return whether component satisfies criteria
   * @todo make it only callable from within checkComponentsOverlap
   */
  def checkCriteria(origData: Array[Double], compData: Array[Double], compNum: Int): Boolean = {
    var area = 0.0
    var cnt = 0
    val maskedTen = compData.map(e => {
      cnt += 1
      if (e == compNum) {
        area += 1.0
        1.0
      } else 0.0
    })
    var dMax = Double.MinValue
    var dMin = Double.MaxValue
    var idx = 0
    /** @todo is there a better soln than initializing it to 0.0? */
    var curVal = 0.0
    while (idx < cnt) {
      if (maskedTen(idx) != 0) {
        curVal = origData(idx)
        if (dMax < curVal) dMax = origData(idx)
        if (dMin > curVal) dMin = origData(idx)
      }
      idx += 1
    }
    ((area >= 2400.0) || ((area < 2400.0) && ((dMin/dMax) > 0.9)))
  }

  /**
   * Whether two components overlap.
   *
   * @param comps1 first component tensor
   * @param comps2 second component tensor
   * @param compNum1 first component number
   * @param compNum2 second component number
   * @return whether the two components overlap
   * @todo this method only makes sense if the two tensors are component tensors,
   * i.e. the values are the component numbers not normal values. make sure that
   * this method is thus only callable with such tensors, e.g. make it only callable
   * from within checkComponentsOverlap.
   */
  def overlap(comps1: AbstractTensor, comps2: AbstractTensor, compNum1: Int, compNum2: Int): Boolean = {
    /** mask for specific component */
    val maskedComp1 = comps1.map(e => {
      if (e == compNum1) 1.0 else 0.0
    })
    val maskedComp2 = comps1.map(e => {
      if (e == compNum2) 1.0 else 0.0
    })
    /** check overlap */
    !(maskedComp1 * maskedComp2).isZeroShortcut
  }

}
