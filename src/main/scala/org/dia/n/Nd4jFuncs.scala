package org.dia.n

import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import org.dia.NetCDFUtils
import org.dia.TRMMUtils.Constants._

import scala.language.implicitConversions

/**
 * Created by rahulsp on 7/6/15.
 */
object Nd4jFuncs {

  /**
   * Gets an NDimensional Array of ND4j
   * @param url
   * @param variable
   * @return
   */
  def getNd4jNetCDFTRMMVars(url : String, variable : String) : INDArray = {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)

    val rowDim = NetCDFUtils.getDimensionSize(netcdfFile, TRMM_ROWS_DIM)
    val columnDim = NetCDFUtils.getDimensionSize(netcdfFile, TRMM_COLS_DIM)

    val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
    val NDArray = Nd4j.create(coordinateArray, Array(rowDim, columnDim))
    NDArray
  }

  /**
   * Gets an NDimensional array of Breeze's DenseMatrix from a NetCDF file
   * TODO :: Need to be able to load in the dimensions of the NetCDF variable on runtime
   * @param largeArray
   * @param blockSize
   * @return
   */
  def getNd4jNetCDFNDVars (url : String, variable : String) : INDArray = {
    null
  }

  def Nd4jReduceResolution(largeArray : INDArray, blockSize : Int) : INDArray = {
    val numRows = largeArray.rows()
    val numCols = largeArray.columns()

    val reducedSize = numRows * numCols / (blockSize * blockSize)

    val reducedMatrix = Nd4j.create(numRows / blockSize, numCols / blockSize)

    for(row <- 0 to reducedMatrix.rows - 1){
      for(col <- 0 to reducedMatrix.columns - 1){
        val rowRange = (row*blockSize to ((row + 1) * blockSize) - 1).toSet
        val columnRange = (col * blockSize to ((col + 1) * blockSize) - 1).toSet
        val crossProductRanges = for { x <- rowRange; y <- columnRange} yield (x, y)
        val block = crossProductRanges.map(pair => largeArray.getDouble(pair._1, pair._2))
        val numNonZero = block.filter(p => p != 0).size
        val sum = block.reduce((A, B) => A + B)
        reducedMatrix.put(row, col, sum / numNonZero)
      }
    }
    reducedMatrix
  }
}
