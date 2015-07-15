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
package org.dia

import org.slf4j.Logger
import ucar.ma2
import ucar.nc2.dataset.NetcdfDataset

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Contains all functions needed to handle netCDF files
 */
object NetCDFUtils {

  // Class logger
  val LOG : Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Loads a NetCDF file from a url
   * TODO :: Check if it loads the entire file with openDataset
   */
  def loadNetCDFDataSet(url : String) : NetcdfDataset = {
    NetcdfDataset.setUseNaNs(false)
    val netcdfFile = NetcdfDataset.openDataset(url)
    netcdfFile
  }

  /**
   * Gets a 1D Java array of Doubles from a netCDFDataset using a variable
   * @param netcdfFile the NetcdfDataSet to read from
   * @param variable the variable array to extract
   * @return
   */
  def convertMa2ArrayTo1DJavaArray(netcdfFile: NetcdfDataset, variable: String): Array[Double] = {
    val SearchVariable: ma2.Array = getNetCDFVariableArray(netcdfFile, variable)
    var coordinateArray: Array[Double] = Array.empty
    val oneDarray = SearchVariable.copyTo1DJavaArray()
    // convert to doubles
    try {
      if (!SearchVariable.copyTo1DJavaArray().isInstanceOf[Array[Double]]) {
        coordinateArray = oneDarray.asInstanceOf[Array[Float]]
          .map(p => p.toDouble)
      }
      //TODO pluggable cleaning values procedures?
      coordinateArray = coordinateArray.map(p => {
        if (p == -9999.0) 0.0 else p
      })
    } catch {
      // Something could go wrong while casting elements
      case ex: Exception =>
        println("Error while converting a netcdf.ucar.ma2 to a 1D array")

    }
    coordinateArray
  }

  /**
   * Gets a M2 array from a netCDF file using a variable
   * @param netcdfFile the NetcdfDataSet to read from
   * @param variable the variable array to extract
   * @return
   */
  def getNetCDFVariableArray(netcdfFile: NetcdfDataset, variable: String): ma2.Array = {
    var SearchVariable: ma2.Array = null
    try {
      SearchVariable = netcdfFile.findVariable(variable).read()
    } catch {
      case ex: Exception =>
        ex.printStackTrace()
    }
    SearchVariable
  }

  /**
   * Gets the row dimension of a specific file
   * @param netcdfFile the NetcdfDataSet to read from
   * @param rowDim the specific dimension to get the size of
   * @return
   */
  def getDimensionSize(netcdfFile : NetcdfDataset, rowDim : String): Int = {
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

  /**
   * Gets the dimension sizes from a list of Dimension
   * Dimension[2] -> rows, latitude
   * Dimension[1] -> cols, longitude
   * Other dimensions start at index 3
   * @param netcdfFile the NetcdfDataSet to read from
   * @param variable the variable array to extract
   * @return
   */
  def getDimensionSizes(netcdfFile: NetcdfDataset, variable: String): mutable.HashMap[Int, Int] = {
    //TODO verify if the variable name actually exists
    val dimensions = netcdfFile.findVariable(variable).getDimensions
    val it = dimensions.iterator
    val dSizes = new mutable.HashMap[Int, Int]()
    var iterate = 3

    while (it.hasNext) {
      val d = it.next()
      println(d)
      if(TRMMUtils.Constants.X_AXIS_NAMES.contains(d.getName.toLowerCase)){
        dSizes.put(2, d.getLength)
      } else if( TRMMUtils.Constants.Y_AXIS_NAMES.contains(d.getName.toLowerCase)){
        dSizes.put(1, d.getLength)
      } else {
        dSizes.put(iterate, d.getLength)
        iterate += 1
      }
    }
    if (dSizes.get(2).equals(None))
      LOG.warn("No X-axis dimension found.")
    if (dSizes.get(1).equals(None))
      LOG.warn("No Y-axis dimension found.")
    LOG.debug("Dimensions found: %s".format(dSizes.toStream))
    dSizes
  }
}