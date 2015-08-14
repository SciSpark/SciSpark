package org.dia.algorithms.mcc

import org.dia.core.sciTensor
import org.dia.tensors.AbstractTensor

import scala.collection.mutable

object mccOps {

  val BACKGROUND = 0.0

  def reduceResolution(tensor: AbstractTensor, blockSize: Int): AbstractTensor = {
    val largeArray = tensor
    val numRows = largeArray.rows
    val numCols = largeArray.cols
    val reducedSize = numRows * numCols / (blockSize * blockSize)

    val reducedMatrix = tensor.zeros(numRows / blockSize, numCols / blockSize)

    for (row <- 0 to (reducedMatrix.rows - 1)) {
      for (col <- 0 to (reducedMatrix.cols - 1)) {
        val rowRange = (row * blockSize) -> ((row + 1) * blockSize)
        val columnRange = (col * blockSize) -> ((col + 1) * blockSize)
        val block = tensor(rowRange, columnRange)
        val numNonZero = block.data.count(p => p != 0)
        val avg = if (numNonZero > 0) block.cumsum / numNonZero else 0.0
        reducedMatrix.put(avg, row, col)
      }
    }
    reducedMatrix
  }

  def findCloudComponents(tensor: sciTensor): List[sciTensor] = {
    val labelledTensors = findConnectedComponents(tensor.tensor)
    val absT: AbstractTensor = tensor.tensor

    val seq = labelledTensors.indices.map(p => {
      val masked: AbstractTensor = labelledTensors(p).map(a => if (a != 0.0) 1.0 else a)
      val areaTuple = areaFilled(masked * absT)
      val area = areaTuple._1
      val max = areaTuple._2
      val min = areaTuple._3
      val metadata = tensor.metaData += (("AREA", "" + area)) += (("DIFFERENCE", "" + (max - min))) += (("COMPONENT", "" + p))
      val k = new sciTensor(tensor.varInUse, masked, metadata)
      k
    })
    seq.toList
  }

  def findConnectedComponents(tensor: AbstractTensor): List[AbstractTensor] = {
    val tuple = labelConnectedComponents(tensor)
    val labelled = tuple._1
    val maxVal = tuple._2
    val maskedLabels = (1 to maxVal).toArray.map(labelled := _.toDouble)
    maskedLabels.toList
  }

  def findCloudElements(tensor: sciTensor): List[sciTensor] = {
    val labelledTensors = findCloudElements(tensor.tensor)
    val absT: AbstractTensor = tensor.tensor

    val seq = (0 to labelledTensors.size - 1).map(p => {
      val masked: AbstractTensor = labelledTensors(p).map(a => if (a != 0.0) 1.0 else a)

      val metaTensor = tensor.tensor * masked
      val max = metaTensor.max
      val min = metaTensor.min
      val area = areaFilled(masked)
      val metadata = tensor.metaData += (("AREA", "" + area)) += (("DIFFERENCE", "" + (max - min))) += (("COMPONENT", "" + p))
      val k = new sciTensor(tensor.varInUse, masked, metadata)
      k
    })
    seq.toList
  }

  def findCloudElements(tensor: AbstractTensor): List[AbstractTensor] = {
    val tuple = labelConnectedComponents(tensor)
    val labelled = tuple._1
    val maxVal = tuple._2
    val maskedLabels = (1 to maxVal).toArray.map(labelled := _.toDouble)
    maskedLabels.toList
  }

  def areaFilled(tensor: AbstractTensor): (Double, Double, Double) = {
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

  def findCloudElementsX(tensor: sciTensor): sciTensor = {
    // list of connected components separated in maskes matrices
    val labelledTensor = findCloudElementsX(tensor.tensor)
    val metadata = tensor.metaData += (("NUM_COMPONENTS", "" + labelledTensor._2))
    new sciTensor(tensor.varInUse, labelledTensor._1, metadata)
  }

  def findCloudElementsX(tensor: AbstractTensor): (AbstractTensor, Int) = {
    val tuple = labelConnectedComponents(tensor)
    tuple
  }

  def labelConnectedComponents(tensor: AbstractTensor): (AbstractTensor, Int) = {
    val fourVector = List((1, 0), (-1, 0), (0, 1), (0, -1))
    val rows = tensor.rows
    val cols = tensor.cols
    val labels = tensor.zeros(tensor.shape: _*)
    var label = 1
    var stack = new mutable.Stack[(Int, Int)]()
    /**
     * If the coordinates are within bounds,
     * the input is not 0, and it hasn't been labelled yet
     * @param row the row to check
     * @param col the column to check
     * @return
     */
    def isLabeled(row: Int, col: Int): Boolean = {
      if (row < 0 || col < 0 || row >= rows || col >= cols) return true
      tensor(row, col) == BACKGROUND || labels(row, col) != BACKGROUND
    }



    def dfs(currentLabel: Int): Unit = {
      while (!stack.isEmpty) {
        val tuple = stack.pop
        val row = tuple._1
        val col = tuple._2
        labels.put(currentLabel, row, col)
        val neighbors = fourVector.map(p => (p._1 + row, p._2 + col))
        for (neighbor <- neighbors) {
          if (!isLabeled(neighbor._1, neighbor._2)) stack.push((neighbor._1, neighbor._2))
        }
      }
    }

    //First Pass
    for (row <- 0 to (rows - 1)) {
      for (col <- 0 to (cols - 1)) {
        if (!isLabeled(row, col)) {
          stack.push((row, col))
          dfs(label)
          label += 1
        }
      }
    }
    stack = null
    (labels, label - 1)
  }

}
