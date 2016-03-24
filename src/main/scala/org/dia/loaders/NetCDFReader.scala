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

import org.dia.utils.NetCDFUtils
import org.slf4j.Logger
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset

/**
 * Utility functions to create a multi-dimensional array from a NetCDF,
 * most importantly from some URI.
 */
object NetCDFReader {

  // Class logger
  val LOG: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Gets a multi-dimensional array from a NetCDF at some URI.
   *
   * @param uri where the NetCDF file is located
   * @param variable the NetCDF variable to extract
   * @return
   */
  def loadNetCDFNDVar(uri: String, variable: String): (Array[Double], Array[Int]) = {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(uri)
    if (netcdfFile == null) {
      LOG.warn("Dataset %s not found!".format(uri))
      return (Array(-9999), Array(1, 1))
    }

    val coordinateArray = NetCDFUtils.netCDFArrayAndShape(netcdfFile, variable)
    val variableArray = coordinateArray._1

    if (variableArray.length < 1) {
      LOG.warn("Variable '%s' in dataset at %s not found!".format(variable, uri))
      return (Array(-9999), Array(1, 1))
    }
    coordinateArray
  }

  /**
   * Gets just the variable names of a NetCDF at some URI.
   *
   * @param uri where the NetCDF file is located
   * @return
   */
  def loadNetCDFVar(uri: String): List[String] = {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(uri)
    val vars = netcdfFile.getVariables
    /** We have to be mutable here because vars is a Java list which has no map function. */
    var list: List[String] = List()
    for (i <- 0 to vars.size - 1) {
      val k = vars.get(i).getShortName
      list ++= List(k)
    }
    list
  }

  /**
   * Loads the NetCDF file based on a variable name
   *
   * @param dataset the dataset to perform a load operation upon
   * @param variable the specific variable within the dataset to load
   * @return
   */
  def loadNetCDFNDVar(dataset: NetcdfDataset, variable: String): (Array[Double], Array[Int]) = {
    if (dataset == null) {
      LOG.warn("Dataset %s not found!".format())
      return (Array(-9999), Array(1, 1))
    }

    val coordinateArray = NetCDFUtils.netCDFArrayAndShape(dataset, variable)
    val variableArray = coordinateArray._1

    if (variableArray.length < 1) {
      LOG.warn("Variable '%s' in dataset not found!".format(variable))
      return (Array(-9999), Array(1, 1))
    }
    coordinateArray
  }

  /**
   * Loads the NetCDF file based on a byte array
   *
   * @param name the file name in question
   * @param file the array of bytes representing the file
   * @return
   */
  def loadNetCDFFile(name: String, file: Array[Byte]): NetcdfDataset = {
    new NetcdfDataset(NetcdfFile.openInMemory(name, file))
  }

}
