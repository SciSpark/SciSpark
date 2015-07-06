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
import org.dia.TRMMUtils.Constants
import Constants._
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

