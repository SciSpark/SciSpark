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

import org.dia.TRMMUtils.Constants._
import org.dia.TRMMUtils.NetCDFUtils
import org.dia.core.ArrayLib
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.api.linalg.{RichNDArray, DSL}
import org.nd4j.linalg.factory.Nd4j
import ucar.nc2.dataset.NetcdfDataset

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * The Nd4j Functional operations
 * Created by rahulsp on 7/6/15.
 */

class Nd4jLib extends ArrayLib[INDArray]{

  val name : String = "nd4j"
  var iNDArray : INDArray
  def this(array : INDArray) {
    this
    iNDArray = array
  }
  // TODO :: Opportunity to refactor loaders
  /**
   * Gets an NDimensional Array of ND4j from a TRMM dataset
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  def loadNetCDFTRMMVars(url: String, variable: String): Unit = {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)

    val rowDim = NetCDFUtils.getDimensionSize(netcdfFile, X_AXIS_NAMES(0))
    val columnDim = NetCDFUtils.getDimensionSize(netcdfFile, Y_AXIS_NAMES(0))

    val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
    iNDArray = Nd4j.create(coordinateArray, Array(rowDim, columnDim))
  }

  /**
   * Gets an NDimensional array of INDArray from a NetCDF url
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  def loadNetCDFNDVars(url: String, variable: String): Unit =  {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)
    loadNetCDFNDVars(netcdfFile, variable)
  }

  /**
   * Gets an NDimensional array of INDArray from a NetCDF file
   * @param netcdfFile where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  def loadNetCDFNDVars(netcdfFile: NetcdfDataset, variable: String): Unit = {
    val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
    val dims = NetCDFUtils.getDimensionSizes(netcdfFile, variable)
    val shape = dims.toArray.sortBy(_._1).map(_._2)
    iNDArray = Nd4j.create(coordinateArray, shape)
    
  }

  /**
   * Creates a 2D array from a list of dimensions using a variable
   * @param dimensionSizes hashmap of (dimension, size) pairs
   * @param netcdfFile the NetcdfDataset to read
   * @param variable the variable array to extract
   * @return DenseMatrix
   */
  def create2dArray(dimensionSizes: mutable.HashMap[Int, Int], netcdfFile: NetcdfDataset, variable: String): Unit = {

    val x = dimensionSizes.get(1).get
    val y = dimensionSizes.get(2).get

    val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)

    iNDArray = Nd4j.create(coordinateArray, Array(x, y))
  }


  def reduceResolution(largeArray: INDArray, blockSize: Int): Nd4jLib = {
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
        val avg = if (numNonZero > 0) (block.sum / numNonZero) else 0.0
        reducedMatrix.put(row, col, avg)
      }
    }
    new Nd4jLib(reducedMatrix)
  }


}
