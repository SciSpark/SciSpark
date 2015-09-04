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

import org.dia.Utils.NetCDFUtils
import org.slf4j.Logger
import ucar.nc2.NetcdfFile
import ucar.nc2.dataset.NetcdfDataset

object NetCDFReader {

  // Class logger
  val LOG: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Gets an NDimensional array of from a NetCDF url
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  def loadNetCDFNDVars(url: String, variable: String): (Array[Double], Array[Int]) = {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)
    if (netcdfFile == null) {
      LOG.warn("Dataset %s not found!".format(url))
      return (Array(-9999), Array(1, 1))
    }

    val coordinateArray = NetCDFUtils.netcdfArrayandShape(netcdfFile, variable)
    val variableArray = coordinateArray._1

    if (variableArray.length < 1) {
      LOG.warn("Variable '%s' in dataset in %s not found!".format(variable, url))
      return (Array(-9999), Array(1, 1))
    }

    coordinateArray
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

  def loadNetCDFNDVars(dataset : NetcdfDataset, variable : String) : (Array[Double], Array[Int]) = {
    if (dataset == null) {
      LOG.warn("Dataset %s not found!".format())
      return (Array(-9999), Array(1, 1))
    }

    val coordinateArray = NetCDFUtils.netcdfArrayandShape(dataset, variable)
    val variableArray = coordinateArray._1

    if (variableArray.length < 1) {
      LOG.warn("Variable '%s' in dataset not found!".format(variable))
      return (Array(-9999), Array(1, 1))
    }
    coordinateArray
  }

  def loadNetCDFFile(name : String, file : Array[Byte]) : NetcdfDataset = {
    new NetcdfDataset(NetcdfFile.openInMemory(name, file))
  }

}
