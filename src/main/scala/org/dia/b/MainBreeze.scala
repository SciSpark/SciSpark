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
import org.apache.spark.{SparkConf, SparkContext}
import org.dia.Constants._
import org.dia.NetCDFUtils
import org.jblas.DoubleMatrix
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import ucar.ma2
import ucar.nc2.dataset.NetcdfDataset

import scala.language.implicitConversions

/**
 * Functions needed to perform operations with Breeze
 */
object MainBreeze {

  /**
   * Breeze implementation
   * The url is the base url where the netcdf file is located.
   * 1) Fetch the variable array from via the NetCDF api
   * 2) Download and convert the netcdf array to 1D array of doubles
   * 3) Reformat the array as a jblas Double Matrix, and reshape it with the original coordinates
   *
   * TODO :: How to obtain the array dimensions from the netcdf api,
   *         instead of hardcoding for reshape function
   * @param url
   * @param variable
   * @return
   */
  def getBreezeNetCDFVars (url : String, variable : String) : DenseMatrix[Double] = {
    var netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)
    var rowDim = NetCDFUtils.getRowDimension(netcdfFile)
    var columnDim = NetCDFUtils.getColDimension(netcdfFile)

    val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
    val matrix = new DenseMatrix[Double](rowDim, columnDim, coordinateArray, 0)
    matrix
  }

  /**
   * Gets an NDimensional array of Breeze's DenseMatrices from a NetCDF file
   * @param url
   * @param variable
   * @return
   */
  def getBreezeNetCDFNDVars (url : String, variable : String) : Array[DenseMatrix[Double]] = {
    var netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)
    var SearchVariable: ma2.Array = NetCDFUtils.getNetCDFVariableArray(netcdfFile, variable)
    val ArrayClass = Array.ofDim[Float](240, 1, 201 ,194)
    val NDArray = SearchVariable.copyToNDJavaArray().asInstanceOf[ArrayClass.type]
    val j = NDArray(0)(0).flatMap(f => f)
    val any = NDArray.map(p => new DenseMatrix[Double](201, 194, p(0).flatMap(f => f).map(d => d.toDouble), 0))
    any
  }

  /**
   * Reduces the resolution of a DenseMatrix
   * @param largeArray
   * @param blockSize
   * @return
   */
  def breezereduceResolution(largeArray : DenseMatrix[Double], blockSize : Int) : DenseMatrix[Double] = {
    val numRows = largeArray.rows
    val numCols = largeArray.cols

    val reducedSize = numRows * numCols / (blockSize * blockSize)
    val reducedMatrix = DenseMatrix.zeros[Double](numRows / blockSize, numCols / blockSize)

    for(row <- 0 to reducedMatrix.rows - 1){
      for(col <- 0 to reducedMatrix.cols - 1){
        val rowIndices = (row * blockSize) to (((row + 1)) * blockSize - 1)
        val colIndices = (col * blockSize) to ((col + 1) * blockSize - 1)
        val block = largeArray(rowIndices, colIndices)
        val totalsum = sum(block)
        val validCount = block.findAll(p => p != 0.0).size.toDouble
        val average = if(validCount > 0) totalsum / validCount else 0.0
        reducedMatrix(row to row, col to col) := average
        reducedMatrix
      }
    }

    reducedMatrix
  }
}

