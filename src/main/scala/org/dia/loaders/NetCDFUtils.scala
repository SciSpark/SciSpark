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
package org.dia.loaders

import org.dia
import org.dia.Constants
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
  val LOG: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Loads a NetCDF file from a url
   * TODO :: Check if it loads the entire file with openDataset
   */
  def loadNetCDFDataSet(url: String): NetcdfDataset = {
    NetcdfDataset.setUseNaNs(false)
    try {
      var netcdfFile = NetcdfDataset.openDataset(url)
      netcdfFile
    } catch {
      case e: java.io.IOException => LOG.error("Couldn't open dataset %s".format(url))
        null
      case ex: Exception => LOG.error("Something went wrong while reading %s".format(url))
        null
    }
  }

  /**
   * Gets a tuple of 1D Java array of Doubles and Array of Ints for shape
   * from a netCDFDataset using a variable
   * @param netcdfFile the NetcdfDataSet to read from
   * @param variable the variable array to extract
   * @return
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

  def convertMa2Arrayto1DJavaArray(ma2Array: ma2.Array): Array[Double] = {
    var array: Array[Double] = Array(-123456789)
    val javaArray = ma2Array.copyTo1DJavaArray()
    try {
      if (!javaArray.isInstanceOf[Array[Double]]) {
        array = javaArray.asInstanceOf[Array[Float]].map(p => p.toDouble)
      }
      array = array.map(p => {
        if (p == -9999.0) 0.0 else p
      })
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
      if (netcdfFile == null)
        throw new IllegalStateException("NetCDFDataset was not loaded")
      val netcdfVal = netcdfFile.findVariable(variable)
      println(netcdfVal)
      if (netcdfVal == null)
        throw new IllegalStateException("Variable '%s' was not loaded".format(variable))
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
   * Gets the row dimension of a specific file
   * @param netcdfFile the NetcdfDataSet to read from
   * @param rowDim the specific dimension to get the size of
   * @return
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
    // if the netcdf doesn't exists
    if (netcdfFile == null)
      return new mutable.HashMap[Int, Int]()

    // if the variable doesn't exist
    var netcdfVariable = netcdfFile.findVariable(variable)
    if (netcdfVariable == null)
      return new mutable.HashMap[Int, Int]()

    var dimensions = netcdfVariable.getDimensions
    val it = dimensions.iterator
    val dSizes = new mutable.HashMap[Int, Int]()
    var iterate = 3

    while (it.hasNext) {
      val d = it.next()
      //TODO verify how the names map to indexes
      if (Constants.X_AXIS_NAMES.contains(d.getName.toLowerCase)) {
        dSizes.put(2, d.getLength)
      } else if (dia.Constants.Y_AXIS_NAMES.contains(d.getName.toLowerCase)) {
        dSizes.put(1, d.getLength)
      } else {
        dSizes.put(iterate, d.getLength)
        iterate += 1
      }
    }
    if (dSizes.get(2).isEmpty)
      LOG.warn("No X-axis dimension found.")
    if (dSizes.get(1).isEmpty)
      LOG.warn("No Y-axis dimension found.")
    LOG.debug("Dimensions found: %s".format(dSizes.toStream))
    dSizes
  }
}