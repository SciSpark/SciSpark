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
package org.dia.Utils

import org.slf4j.Logger
import orq.dia.HDFSRandomAccessFile
import ucar.ma2
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset
import scala.language.implicitConversions

/**
 * Contains all functions needed to handle netCDF files.
 * Note that NaN's are not used for missing values.
 * Instead the value -9999.0 is used to indicate missing values.
 */
object NetCDFUtils {

  // Class logger
  val LOG: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)


  /**
   * Gets a tuple of 1D Java array of Doubles and Array of Ints for shape
   * from a netCDFDataset using a variable. If a variable is not found then an Array with the element 0.0
   * is returned with shape (1,1). Note that all dimensions of size 1 are elimanated.
   * For example the array shape (21, 5, 1, 2) is reduced to (21, 5, 2) since the 3rd dimension
   * has only one index in it.
   */
  def netcdfArrayandShape(netcdfFile: NetcdfDataset, variable: String): (Array[Double], Array[Int]) = {
    val SearchVariableArray: ma2.Array = getNetCDFVariableArray(netcdfFile, variable)
    if (SearchVariableArray == null) {
      LOG.error("Variable '%s' not found. Can't create array. Returning empty array.".format(variable))
      return (Array(0.0), Array(1, 1))
    }
    val nativeArray = convertMa2Arrayto1DJavaArray(SearchVariableArray)
    val shape = SearchVariableArray.getShape.toList.filter(p => p != 1).toArray
    (nativeArray, shape)
  }

  /**
   * Converts the native ma2.Array from the NetCDF library to a one dimensional
   * Java Array of Doubles. Two copies of the array are made, since NetCDF
   * does not have any api to tell what type the arrays are. Once the initial
   * array of the loading is completed a type check is used to appropriatley
   * convert the values into doubles. This involves a second copy
   */
  def convertMa2Arrayto1DJavaArray(ma2Array: ma2.Array): Array[Double] = {
    var array: Array[Double] = Array(-123456789)
    // First copy of array
    val javaArray = ma2Array.copyTo1DJavaArray()

    try {
      // Second copy of Array
      if (!javaArray.isInstanceOf[Array[Double]]) {
        array = javaArray.asInstanceOf[Array[Float]].map(p => p.toDouble)
      } else {
        array = javaArray.asInstanceOf[Array[Double]]
      }

      for (i <- array.indices) if (array(i) == -9999.0) array(i) = 0.0

    } catch {
      case ex: Exception =>
        println("Error while converting a netcdf.ucar.ma2 to a 1D array. Most likely occurred with casting")
    }
    array
  }

  /**
   * Gets a M2 array from a netCDF file using a variable
   *
   * TODO :: Ask why OCW hard codes the lattitude and longitude names
   *
   * @param netcdfFile the NetcdfDataSet to read from
   * @param variable the variable array to extract
   * @return
   */
  def getNetCDFVariableArray(netcdfFile: NetcdfDataset, variable: String): ma2.Array = {
    var SearchVariable: ma2.Array = null
    try {
      if (netcdfFile == null) throw new IllegalStateException("NetCDFDataset was not loaded")
      val netcdfVal = netcdfFile.findVariable(variable)
      if (netcdfVal == null) throw new IllegalStateException("Variable '%s' was not loaded".format(variable))
      SearchVariable = netcdfVal.read()
    } catch {
      case ex: Exception =>
        LOG.error("Variable '%s' not found when reading source %s.".format(variable, netcdfFile))
        LOG.info("Variables available: " + netcdfFile.getVariables)
        LOG.error(ex.getMessage)
    }
    SearchVariable
  }


  /**
   * Loads a NetCDF Dataset from a url
   */
  def loadNetCDFDataSet(url: String): NetcdfDataset = {
    NetcdfDataset.setUseNaNs(false)
    try {
      NetcdfDataset.openDataset(url)
    } catch {
      case e: java.io.IOException => LOG.error("Couldn't open dataset %s".format(url))
        null
      case ex: Exception => LOG.error("Something went wrong while reading %s".format(url))
        null
    }
  }

  /**
    * Loads a NetCDF Dataset from HDFS
    * @param dfsUri   HDFS URI(eg. hdfs://master:9000/)
    * @param location file path on hdfs
    * @param bufferSize the size of buffer to be used
    */
  def loadDFSNetCDFDataSet(dfsUri:String,location:String,bufferSize:Int):NetcdfDataset = {
    NetcdfDataset.setUseNaNs(false)
    try{
      val raf = new HDFSRandomAccessFile(dfsUri,location,bufferSize)
      new NetcdfDataset(NetcdfFile.open(raf,location,null,null))
    } catch {
      case e: java.io.IOException => LOG.error("Couldn't open dataset %s%s".format(dfsUri,location))
        null
      case ex: Exception => LOG.error("Something went wrong while reading %s%s".format(dfsUri,location))
        null
    }
  }


  /**
   * Gets the dimension value of a specific files dimension name
   */
  def getDimensionSize(netcdfFile: NetcdfDataset, rowDim: String): Int = {
    var dimSize = -1
    val it = netcdfFile.getDimensions.iterator()
    while (it.hasNext) {
      val d = it.next()
      if (d.getName.equals(rowDim))
        dimSize = d.getLength
    }
    if (dimSize < 0)
      throw new IllegalStateException("Dimension does not exist!!!")
    dimSize
  }


}