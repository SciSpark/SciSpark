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

import org.dia.Constants._
import org.slf4j.Logger
import ucar.nc2.dataset.NetcdfDataset

import scala.collection.mutable

object NetCDFLoader {
  // Class logger
  val LOG: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Gets an NDimensional Array of ND4j from a TRMM tensors
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  def loadNetCDFTRMMVars(url: String, variable: String): (Array[Double], Array[Int]) = {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)
    val rowDim = NetCDFUtils.getDimensionSize(netcdfFile, X_AXIS_NAMES(0))
    val columnDim = NetCDFUtils.getDimensionSize(netcdfFile, Y_AXIS_NAMES(0))

    val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)

    (coordinateArray, Array(rowDim, columnDim))
  }

  /**
   * Gets an NDimensional array of INDArray from a NetCDF url
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  def loadNetCDFNDVars(url: String, variable: String): (Array[Double], Array[Int]) = {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)

    if (netcdfFile != null) {
      val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
      if (coordinateArray.length > 0) {
        val dims = NetCDFUtils.getDimensionSizes(netcdfFile, variable)
        val shape = dims.toArray.sortBy(_._1).map(_._2)
        return (coordinateArray, shape)
      }
      LOG.warn("Variable '%s' in dataset in %s not found!".format(variable, url))
      return (Array(-9999), Array(1, 1))
    }
    LOG.warn("Variable '%s' in dataset in %s not found!".format(variable, url))
    return (Array(-9999), Array(1, 1))
  }

  def loadNetCDFVariables(url: String): List[String] = {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)
    val variables = netcdfFile.getVariables
    var list: List[String] = List()
    for (i <- 0 to variables.size - 1) {
      val k = variables.get(i).getName
      list ++= List(k)
    }
    list
  }

  /**
   * Creates a 2D array from a list of dimensions using a variable
   * @param dimensionSizes hashmap of (dimension, size) pairs
   * @param netcdfFile the NetcdfDataset to read
   * @param variable the variable array to extract
   * @return DenseMatrix
   */
  def create2dArray(dimensionSizes: mutable.HashMap[Int, Int], netcdfFile: NetcdfDataset, variable: String): (Array[Double], Array[Int]) = {

    val x = dimensionSizes.get(1).get
    val y = dimensionSizes.get(2).get

    val coordinateArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, variable)
    (coordinateArray, Array(x, y))
  }
}
