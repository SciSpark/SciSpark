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

import org.dia.Constants._
import org.slf4j.Logger
import ucar.ma2
import ucar.nc2.dataset.NetcdfDataset

import scala.language.implicitConversions

/**
 * Contains all functions needed to handle netCDF files
 */
object NetCDFUtils {

  // Class logger
  val LOG : Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Loads a NetCDF file from a url
   */
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
   * Gets the row dimension of a specific file
   * @param netcdfFile
   * @return
   */
  def getRowDimension(netcdfFile : NetcdfDataset) : Int = {
    LOG.warn("Using default row dimension name " + ROWS_DIM)
    return getRowDimension(netcdfFile, ROWS_DIM)
  }

  /**
   * Gets the row dimension of a specific file
   * @param netcdfFile
   * @param rowDim
   * @return
   */
  def getRowDimension(netcdfFile : NetcdfDataset, rowDim : String): Int = {
    var dimSize = -1
    val it = netcdfFile.getDimensions.iterator()
    while (it.hasNext) {
      var d = it.next()
      if (d.getName.equals(rowDim))
        dimSize = d.getLength
    }
    if (dimSize < 0)
      throw new IllegalStateException("Dimension does not exist!!!")
    return dimSize
  }

  /**
   * Gets the col dimension of a specific file
   * @param netcdfFile
   * @return
   */
  def getColDimension(netcdfFile : NetcdfDataset) : Int = {
    LOG.warn("Using default row dimension name " + COLS_DIM)
    return getColDimension(netcdfFile, COLS_DIM)
  }

  /**
   * Gets the col dimension of a specific file
   * @param netcdfFile
   * @return
   */
  def getColDimension(netcdfFile : NetcdfDataset, colsDim : String) : Int = {
    val it = netcdfFile.getDimensions.iterator()
    while (it.hasNext) {
      var d = it.next()
      if (d.getName.equals(colsDim))
        return d.getLength
    }
    return DEFAULT_TRMM_COL_SIZE
  }
}