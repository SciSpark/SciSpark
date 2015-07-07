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
package org.dia.n

import org.dia.NetCDFUtils
import org.dia.TRMMUtils.Constants._
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import ucar.nc2.dataset.NetcdfDataset

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * The Nd4j Functional operations
 * Created by rahulsp on 7/6/15.
 */
object Nd4jFuncs {

  /**
   * Gets an NDimensional Array of ND4j
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  def getNd4jNetCDFTRMMVars(url : String, variable : String) : INDArray = {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)

    val rowDim = NetCDFUtils.getDimensionSize(netcdfFile, TRMM_Y_AXIS_NAMES(0))
    val columnDim = NetCDFUtils.getDimensionSize(netcdfFile, TRMM_X_AXIS_NAMES(0))

    val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
    val NDArray = Nd4j.create(coordinateArray, Array(rowDim, columnDim))
    NDArray
  }

  /**
   * Creates a 2D array from a list of dimensions using a variable
   * @param dimensionSizes hashmap of (dimension, size) pairs
   * @param netcdfFile the NetcdfDataset to read
   * @param variable the variable array to extract
   * @return DenseMatrix
   */
  def create2dNd4jArray(dimensionSizes: mutable.HashMap[Int, Int], netcdfFile: NetcdfDataset, variable: String): INDArray = {

    val x = dimensionSizes.get(1).get
    val y = dimensionSizes.get(2).get

    val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)

    Nd4j.create(coordinateArray, Array(x, y))
  }

  /**
   * Gets an NDimensional array of INDArray from a NetCDF file
   * TODO :: Need to be able to load in the dimensions of the NetCDF variable on runtime
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  def getNd4jNetCDFNDVars (url : String, variable : String) : INDArray = {
    null
  }

  def reduceResolution(largeArray: INDArray, blockSize: Int): INDArray = {
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
        val numNonZero = block.count(p => p != 0)
        val sum = block.sum
        reducedMatrix.put(row, col, sum / numNonZero)
      }
    }
    reducedMatrix
  }
}
