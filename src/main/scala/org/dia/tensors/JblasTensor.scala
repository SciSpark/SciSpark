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
package org.dia.tensors

import org.apache.spark.{SparkConf, SparkContext}
import org.dia.TRMMUtils.NetCDFUtils
import org.jblas.DoubleMatrix
import ucar.ma2
import ucar.nc2.dataset.NetcdfDataset

import scala.language.implicitConversions

/**
 * Functions needed to perform operations with JBlas
 */
class JblasTensor {

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
    var netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)
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
   * Gets a 1D Java array of Doubles from a netCDFDataset using a variable
   * @param netcdfFile
   * @param variable
   * @return
   */
  def convertMa2ArrayTo1DJavaArray(netcdfFile : NetcdfDataset, variable : String) : Array[Double] = {
    val SearchVariable: ma2.Array = NetCDFUtils.getNetCDFVariableArray(netcdfFile, variable)
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

//    val HighResolutionArray = urlRDD.map(url => getNd4jNetCDFVars(url, DATASET_VARS.get("ncml").toString))
//    val nanoAfter = System.nanoTime()
//    val LowResolutionArray = HighResolutionArray.map(largeArray => Nd4jReduceResolution(largeArray, 5)).collect
//    LowResolutionArray.map(array => println(array))
  }
}

