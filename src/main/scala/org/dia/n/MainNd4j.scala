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

import breeze.linalg.{DenseMatrix, sum}
import org.apache.spark.{SparkConf, SparkContext}
import org.dia.Constants._
import org.jblas.DoubleMatrix
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import ucar.ma2
import ucar.nc2.dataset.NetcdfDataset

import scala.language.implicitConversions

/**
 * * Functions needed to perform operations with Nd4j
 */
object MainNd4j {

  /**
   * NetCDF variables to use
   * TODO:: Make the netcdf variables global - however this may need broadcasting
   */
  val rowDim = 180
  val columnDim = 360
  val TextFile = "TestLinks"


  /**
   * JBLAS implementation
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
  def getJblasNetCDFVars (url : String, variable : String) : DoubleMatrix = {
    var netcdfFile = loadNetCDFDataSet(url)
    val SearchVariable: ma2.Array = netcdfFile.findVariable(variable).read()

    val coordinateArray = SearchVariable.copyTo1DJavaArray().asInstanceOf[Array[Float]].map(p =>{
      var v = p.toDouble
      if(v == Double.NaN) v = 0
      v
    } )
    val matrix = new DoubleMatrix(coordinateArray).reshape(rowDim, columnDim)
    matrix
  }


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
    var netcdfFile = loadNetCDFDataSet(url)
    val coordinateArray = convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
    val matrix = new DenseMatrix[Double](rowDim,columnDim, coordinateArray, 0)
    matrix
  }

  def loadNetCDFDataSet(url : String) : NetcdfDataset = {
    NetcdfDataset.setUseNaNs(false)
    val netcdfFile = NetcdfDataset.openDataset(url);
    return netcdfFile
  }

  /**
   * Gets a M2 array from a netCDF file using a variable
   * @param netcdfFile
   * @param variable
   * @return
   */
  def getNetCDFVariableArray(netcdfFile : NetcdfDataset, variable : String) : ma2.Array = {
    var SearchVariable: ma2.Array = null
    try {
      SearchVariable = netcdfFile.findVariable(variable).read()
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    }
    return SearchVariable
  }

  /**
   * Gets a 1D Java array of Doubles from a netCDFDataset using a variable
   * @param netcdfFile
   * @param variable
   * @return
   */
  def convertMa2ArrayTo1DJavaArray(netcdfFile : NetcdfDataset, variable : String) : Array[Double] = {
    var SearchVariable: ma2.Array = getNetCDFVariableArray(netcdfFile, variable)
    var coordinateArray : Array[Double] = Array.empty
      coordinateArray = SearchVariable.copyTo1DJavaArray()
        .asInstanceOf[Array[Float]]
        .map(p => {
          var v = p.toDouble
          v = if(v == -9999.0) 0.0 else v
          v
      })
    return coordinateArray
  }

  /**
   * Gets a Ndj4 array from a netCDF file using a variable
   * @param url
   * @param variable
   * @return
   */
  def getNd4jNetCDFVars(url : String, variable : String) : INDArray = {
    var netcdfFile = loadNetCDFDataSet(url)
    val coordinateArray = convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
    val rows = getRowDimension(netcdfFile)
    val cols = getColDimension(netcdfFile)
    val NDarray = Nd4j.create(coordinateArray, Array(rows, cols))
    NDarray
  }

  /**
   * Gets the row dimension of a specific file
   * @param netcdfFile
   * @return
   */
  def getRowDimension(netcdfFile : NetcdfDataset) : Int = {
    val it = netcdfFile.getDimensions.iterator()
    while (it.hasNext) {
      var d = it.next()
      if (d.getName.equals(TRMM_ROWS_DIM))
        return d.getLength
    }
    return DEFAULT_TRMM_ROW_SIZE
  }

  /**
   * Gets the col dimension of a specific file
   * @param netcdfFile
   * @return
   */
  def getColDimension(netcdfFile : NetcdfDataset) : Int = {
    val it = netcdfFile.getDimensions.iterator()
    while (it.hasNext) {
      var d = it.next()
      if (d.getName.equals(TRMM_COLS_DIM))
        return d.getLength
    }
    return DEFAULT_TRMM_COL_SIZE
  }

  def getBreezeNetCDFNDVars (url : String, variable : String) : Array[DenseMatrix[Double]] = {
    var netcdfFile = loadNetCDFDataSet(url)
    var SearchVariable: ma2.Array = getNetCDFVariableArray(netcdfFile, variable)
    val ArrayClass = Array.ofDim[Float](240, 1, 201 ,194)
    val NDArray = SearchVariable.copyToNDJavaArray().asInstanceOf[ArrayClass.type]
    val j = NDArray(0)(0).flatMap(f => f)
    val any = NDArray.map(p => new DenseMatrix[Double](201, 194, p(0).flatMap(f => f).map(d => d.toDouble), 0))
    any
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

  /**
   * 
   * @param largeArray
   * @param blockSize
   * @return
   */
  def jblasreduceResolution(largeArray : DoubleMatrix, blockSize : Int) : DoubleMatrix =  {
    val numRows = largeArray.rows
    val numCols = largeArray.columns

    val reducedSize = numRows * numCols / (blockSize * blockSize)

    val reducedMatrix = DoubleMatrix.zeros(numRows / blockSize, numCols / blockSize)
    for(row <- 0 to reducedMatrix.rows - 1){
      for(col <- 0 to reducedMatrix.columns - 1){
        val block = largeArray.getRange(row * blockSize, ((row + 1) * blockSize) , col * blockSize,  ((col + 1) * blockSize) )
        val average = block.mean
        reducedMatrix.put(row, col, average)
      }
    }

    reducedMatrix
  }

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

  def main(args : Array[String]) : Unit = {
    var cores = Runtime.getRuntime().availableProcessors() - 1;
    //TODO the number of threads should be configured at cluster level
    val conf = new SparkConf().setAppName("L").setMaster("local[" + cores + "]")
    val sparkContext = new SparkContext(conf)
    val urlRDD = sparkContext.textFile(TextFile).repartition(cores)
    //val urlRDD = Source.fromFile(new File(TextFile)).mkString.split("\n")
    /**
     * Uncomment this line in order to test on a normal scala array
     * val urlRDD = Source.fromFile("TestLinks").mkString.split("\n")
     */

    val HighResolutionArray = urlRDD.map(url => getNd4jNetCDFVars(url, DATASET_VARS.get("ncml").toString))
    val nanoAfter = System.nanoTime()
    val LowResolutionArray = HighResolutionArray.map(largeArray => Nd4jReduceResolution(largeArray, 5)).collect
    LowResolutionArray.map(array => println(array))
  }
}

