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
package org.dia.b

import breeze.linalg.DenseMatrix
import org.dia.NetCDFUtils
import ucar.ma2
import ucar.nc2.{NetcdfFile, Dimension}
import ucar.nc2.dataset.NetcdfDataset
import scala.collection.JavaConverters._
import scala.collection.mutable.MutableList

/**
 * Tests for the Breeze functions
  */
class BreezeFuncsTest extends org.scalatest.FunSuite {

  val dailyTrmmUrl = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_L3/TRMM_3B42_daily/1997/365/3B42_daily.1998.01.01.7.bin"
  val hourlyTrmmUrl = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_3Hourly_3B42/1997/365/3B42.19980101.00.7.HDF.Z"
  val knmiUrl = "http://zipper.jpl.nasa.gov/dist/AFRICA_KNMI-RACMO2.2b_CTL_ERAINT_MM_50km_1989-2008_tasmax.nc"
  
  val KMNI_BNDS_DIMENSION = "bnds"
  val KNMI_TASMAX_VAR = "tasmax"
  val DAILY_TRMM_DATA_VAR = "data"
  val HOURLY_TRMM_DATA_VAR = "precipitation"

  // test for creating a N-Dimensional array from daily collected TRMM data
  test("ReadingDailyTRMMDimensions") {
//    var netcdfFile = NetCDFUtils.loadNetCDFDataSet(dailyTrmmUrl)
//    val coordArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, DAILY_TRMM_DATA_VAR)
//    val ExpectedClass = new DenseMatrix[Double](240, 1, coordArray)
//    var dSizes = NetCDFUtils.getDimensionSizes(netcdfFile.findVariable(DAILY_TRMM_DATA_VAR).getDimensions)
//    println("[%s] Dimensions for daily TRMM  data set %s".format("ReadingTRMMDimensions", dSizes.toString()))
//    val resDenseMatrix = BreezeFuncs.create2dBreezeArray(dSizes, netcdfFile, DAILY_TRMM_DATA_VAR)
//    assert(resDenseMatrix.getClass.equals(ExpectedClass.getClass))
//    println()
//    assert(true)
  }

  // test for creating a N-Dimensional array from hourly collected TRMM data
  test("ReadingHourlyTRMMDimensions") {
    var netcdfFile = NetCDFUtils.loadNetCDFDataSet(hourlyTrmmUrl)
    val coordArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, HOURLY_TRMM_DATA_VAR)
    val ExpectedClass = new DenseMatrix[Double](240, 1, coordArray)
    var dSizes = NetCDFUtils.getDimensionSizes(netcdfFile.findVariable(HOURLY_TRMM_DATA_VAR).getDimensions)
    println("[%s] Dimensions for hourly TRMM data set %s".format("ReadingTRMMDimensions", dSizes.toString()))
    val resDenseMatrix = BreezeFuncs.create2dBreezeArray(dSizes, netcdfFile, HOURLY_TRMM_DATA_VAR)
    assert(resDenseMatrix.getClass.equals(ExpectedClass.getClass))
    println()
    assert(true)
  }

  // test for creating a N-Dimension array from KNMI data
  test("ReadingKNMIDimensions") {
//    var netcdfFile = NetCDFUtils.loadNetCDFDataSet(knmiUrl)
//    val ExpectedType = Array.ofDim[Float](240, 1, 201 ,194)
//    var dSizes = NetCDFUtils.getDimensionSizes(netcdfFile.findVariable(KNMI_TASMAX_VAR).getDimensions)
//    println("[%s] Dimensions for KNMI data set %s".format("ReadingKMIDimensions", dSizes.toString()))
//    println()
//    var fdArray = BreezeFuncs.created4dBreezeArray(dSizes, netcdfFile, KNMI_TASMAX_VAR)
//    println()
//    assert(fdArray.getClass.equals(ExpectedType.getClass))
    //TODO verify the sizes of the arrays?
    //println("--------->" + ExpectedType.indices.size)
  }

}
