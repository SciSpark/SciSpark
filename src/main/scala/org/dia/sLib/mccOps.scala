package org.dia.sLib

import org.dia.tensors.AbstractTensor

object mccOps {

  def reduceResolution(tensor: AbstractTensor, blockSize: Int): AbstractTensor = {
    val largeArray = tensor
    val numRows = largeArray.rows
    val numCols = largeArray.cols
    val reducedSize = numRows * numCols / (blockSize * blockSize)

    val reducedMatrix = tensor.zeros(numRows / blockSize, numCols / blockSize)

    for (row <- 0 to (reducedMatrix.rows - 1)) {
      for (col <- 0 to (reducedMatrix.cols - 1)) {
        val rowRange = (row * blockSize) -> (((row + 1) * blockSize))
        val columnRange = (col * blockSize) -> (((col + 1) * blockSize))
        val block = tensor(rowRange, columnRange)
        val numNonZero = block.data.filter(p => p != 0).size
        val avg = if (numNonZero > 0) (block.cumsum / numNonZero) else 0.0
        reducedMatrix.put(avg, row, col)
      }
    }
    reducedMatrix
  }

  def findConnectedComponents(tensor: AbstractTensor): List[AbstractTensor] = {
    null
  }
}
