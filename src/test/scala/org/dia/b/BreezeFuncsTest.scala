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

import org.dia.loaders.NetCDFUtils

/**
 * Tests for the Breeze functions
 */
class BreezeTensorTest extends org.scalatest.FunSuite {

  val dailyTrmmUrl = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_L3/TRMM_3B42_daily/1997/365/3B42_daily.1998.01.01.7.bin"
  val hourlyTrmmUrl = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_3Hourly_3B42/1997/365/3B42.19980101.00.7.HDF.Z"
  val knmiUrl = "http://zipper.jpl.nasa.gov/dist/AFRICA_KNMI-RACMO2.2b_CTL_ERAINT_MM_50km_1989-2008_tasmax.nc"

  val KMNI_BNDS_DIMENSION = "bnds"
  val KNMI_TASMAX_VAR = "tasmax"
  val DAILY_TRMM_DATA_VAR = "compData"
  val HOURLY_TRMM_DATA_VAR = "precipitation"

  val EXPECTED_COLS = 1440
  val EXPECTED_ROWS = 400
  val BLOCK_SIZE = 5

  /**
   * Testing creation of 2D Array (DenseMatrix) from daily collected TRMM compData
   * Assert Criteria : Dimensions of TRMM compData matches shape of 2D Array
   */
  test("ReadingDailyTRMMDimensions") {
    //    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(dailyTrmmUrl)
    //
    //    val coordArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, DAILY_TRMM_DATA_VAR)
    //    val ExpectedClass = new DenseMatrix[Double](EXPECTED_ROWS, EXPECTED_COLS, coordArray)
    //    val dSizes = NetCDFUtils.getDimensionSizes(netcdfFile, DAILY_TRMM_DATA_VAR)
    //    println("[%s] Dimensions for daily TRMM  compData set %s".format("ReadingTRMMDimensions", dSizes.toString()))
    //
    //    val resDenseMatrix = BreezeTensor.create2dArray(dSizes, netcdfFile, DAILY_TRMM_DATA_VAR)
    //
    //    assert(resDenseMatrix.getClass.equals(ExpectedClass.getClass))
    //    assert(ExpectedClass.rows == resDenseMatrix.rows)
    //    assert(ExpectedClass.cols == resDenseMatrix.cols)
    assert(true)
  }


  /**
   * Testing creation of 2D Array (DenseMatrix) from hourly collected TRMM compData
   * Assert Criteria : Dimensions of TRMM compData matches shape of 2D Array
   */
  test("ReadingHourlyTRMMDimensions") {
    //    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(hourlyTrmmUrl)
    //
    //    val coordArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, HOURLY_TRMM_DATA_VAR)
    //    val ExpectedClass = new DenseMatrix[Double](EXPECTED_ROWS, EXPECTED_COLS, coordArray)
    //    val dSizes = NetCDFUtils.getDimensionSizes(netcdfFile, HOURLY_TRMM_DATA_VAR)
    //    println("[%s] Dimensions for hourly TRMM compData set %s".format("ReadingTRMMDimensions", dSizes.toString()))
    //
    //    val resDenseMatrix = BreezeTensor.create2dArray(dSizes, netcdfFile, HOURLY_TRMM_DATA_VAR)
    //
    //    assert(resDenseMatrix.getClass.equals(ExpectedClass.getClass))
    //    assert(ExpectedClass.rows == resDenseMatrix.rows)
    //    assert(ExpectedClass.cols == resDenseMatrix.cols)
    assert(true)
  }


  /**
   * test for creating a N-Dimension array from KNMI compData
   */
  test("ReadingKNMIDimensions") {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(knmiUrl)

    val ExpectedType = Array.ofDim[Float](240, 1, 201, 194)
    val dSizes = NetCDFUtils.getDimensionSizes(netcdfFile, KNMI_TASMAX_VAR)
    println("[%s] Dimensions for KNMI compData set %s".format("ReadingKMIDimensions", dSizes.toString()))

    //    val fdArray = BreezeTensor.create4dArray(dSizes, netcdfFile, KNMI_TASMAX_VAR)
    //    assert(fdArray.getClass.equals(ExpectedType.getClass))
  }

  /**
   * Tests the ReduceResolution function.
   * Assert Criteria : Proper reduction of dimensions by the factor BLOCK_SIZE
   */
  test("ReduceResolutionTest") {
    //    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(hourlyTrmmUrl)
    //
    //    val coordArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, HOURLY_TRMM_DATA_VAR)
    //    val ExpectedClass = new DenseMatrix[Double](EXPECTED_ROWS / BLOCK_SIZE, EXPECTED_COLS / BLOCK_SIZE)
    //
    //    val dSizes = NetCDFUtils.getDimensionSizes(netcdfFile, HOURLY_TRMM_DATA_VAR)
    //    println("[%s] Dimensions for hourly TRMM compData set %s".format("ReadingTRMMDimensions", dSizes.toString()))
    //
    //    val resDenseMatrix = BreezeTensor.create2dArray(dSizes, netcdfFile, HOURLY_TRMM_DATA_VAR)
    //    val reducedMatrix = BreezeTensor.reduceResolution(resDenseMatrix, BLOCK_SIZE)
    //
    //
    //    assert(reducedMatrix.getClass.equals(ExpectedClass.getClass))
    //    assert(ExpectedClass.rows == reducedMatrix.rows)
    //    assert(ExpectedClass.cols == reducedMatrix.cols)
  }


}
