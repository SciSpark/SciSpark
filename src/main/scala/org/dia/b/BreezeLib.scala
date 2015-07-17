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
package org.dia.b

import breeze.linalg.{DenseMatrix, sum}
import org.dia.TRMMUtils.Constants._
import org.dia.TRMMUtils.NetCDFUtils
import org.dia.core.ArrayLib
import ucar.nc2.dataset.NetcdfDataset

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Functions needed to perform operations with Breeze
 * We map every dimension to an index ex : dimension 1 -> Int 1, dimension 2 -> Int 2 etc.
 */
class BreezeLib(val tensor : DenseMatrix[Double]) extends ArrayLib {
  type  T = BreezeLib
  val name : String = "breeze"

  /**
   * Breeze implementation for loading TRMM data
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  def loadNetCDFTRMMVars (url : String, variable : String) : BreezeLib = {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)

    val rowDim = NetCDFUtils.getDimensionSize(netcdfFile, X_AXIS_NAMES(0))
    val columnDim = NetCDFUtils.getDimensionSize(netcdfFile, Y_AXIS_NAMES(0))

    val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
    new BreezeLib(new DenseMatrix[Double](rowDim, columnDim, coordinateArray, 0))
  }

  /**
   * Gets an NDimensional array of Breeze's DenseMatrices from a NetCDF file
   * TODO :: How do we return nested DenseMatrices - given the function return type has to match T
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   *
   */
  def loadNetCDFNDVars (url : String, variable : String) : BreezeLib = {
//    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)
//    val SearchVariable: ma2.Array = NetCDFUtils.getNetCDFVariableArray(netcdfFile, variable)
//    val ArrayClass = Array.ofDim[Float](240, 1, 201 ,194)
//    val NDArray = SearchVariable.copyToNDJavaArray().asInstanceOf[ArrayClass.type]
//     we can only do this because the height dimension is 1
//    val j = NDArray(0)(0).flatMap(f => f)
//    val any = NDArray.map(p => new DenseMatrix[Double](201, 194, p(0).flatMap(f => f).map(d => d.toDouble), 0))
//    denseMatrix = any
    null.asInstanceOf[BreezeLib]
  }

  /**
   * Reduces the resolution of a DenseMatrix
   * @param blockSize the size of n x n size of blocks.
   * @return
   */
  def reduceResolution (blockSize: Int): BreezeLib = {
    val largeArray = tensor
    val numRows = largeArray.rows
    val numCols = largeArray.cols

    val reducedSize = numRows * numCols / (blockSize * blockSize)
    val reducedMatrix = DenseMatrix.zeros[Double](numRows / blockSize, numCols / blockSize)

    for(row <- 0 to reducedMatrix.rows - 1){
      for(col <- 0 to reducedMatrix.cols - 1){
        val rowIndices = (row * blockSize) to ((row + 1) * blockSize - 1)
        val colIndices = (col * blockSize) to ((col + 1) * blockSize - 1)
        val block = largeArray(rowIndices, colIndices)
        val totalsum = sum(block)
        val validCount = block.findAll(p => p != 0.0).size.toDouble
        val average = if(validCount > 0) totalsum / validCount else 0.0
        reducedMatrix(row to row, col to col) := average
        reducedMatrix
      }
    }
    new BreezeLib(reducedMatrix)
  }

  /**
   * Creates a 2D array from a list of dimensions using a variable
   * @param dimensionSizes hashmap of (dimension, size) pairs
   * @param netcdfFile the NetcdfDataset to read
   * @param variable the variable array to extract
   * @return DenseMatrix
   */
  def create2dArray (dimensionSizes: mutable.HashMap[Int, Int], netcdfFile: NetcdfDataset, variable: String): BreezeLib = {
    //TODO make sure that the dimensions are always in the order we want them to be
    try {
      val x = dimensionSizes.get(1).get
      val y = dimensionSizes.get(2).get
      val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
      new BreezeLib(new DenseMatrix[Double](x, y, coordinateArray))
    } catch {
      case e :
        java.util.NoSuchElementException => LOG.error("Required dimensions not found. Found:%s".format(dimensionSizes.toString()))
        null
    }
  }

  def +(array: BreezeLib): BreezeLib = {
    val sum = array.tensor + tensor
    new BreezeLib(sum)
  }

}

