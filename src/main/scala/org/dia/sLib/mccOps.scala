package org.dia.sLib

import org.dia.tensors.AbstractTensor

object mccOps {

  val BACKGROUND=0.0

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

  def labelConnectedComponents(tensor: AbstractTensor): AbstractTensor = {
    val fourVector = List((1,0), (-1,0), (0,1), (0,-1))
    val rows = tensor.rows
    val cols = tensor.cols
    val labels = tensor.zeros(tensor.shape :_*)
    var label = 1

    /**
     * If the coordinates are within bounds,
     * the input is not 0, and it hasn't been labelled yet
     * @param row
     * @param col
     * @return
     */
    def isLabeled(row : Int, col : Int) : Boolean = {
      if(row < 0 || col < 0 || row >= rows || col >= cols) return true
      tensor(row, col) == BACKGROUND || labels(row, col) != BACKGROUND
    }

    def dfs(row : Int, col : Int, currentLabel : Int) : Unit = {
      if(isLabeled(row, col)) return
      labels.put(currentLabel, row, col)

      val neighbors = fourVector.map(p => (p._1 + row, p._2 + col))
      for(neighbor <- neighbors) dfs(neighbor._1, neighbor._2, currentLabel)
    }

    //First Pass
    for(row <- 0 to (rows - 1)) {
      for(col <- 0 to (cols - 1)) {
          if(!isLabeled(row, col)) {
            dfs(row, col, label)
            label += 1
          }
        }
      }
    labels
  }

  def findConnectedComponents(tensor : AbstractTensor) : List[AbstractTensor] = {
    val labelled = labelConnectedComponents(tensor)

  }
}
