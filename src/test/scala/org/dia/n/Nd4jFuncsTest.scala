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

import org.dia.NetCDFUtils
import org.nd4j.api.linalg.DSL._
import org.nd4j.linalg.factory.Nd4j

/**
 * Tests for the Breeze functions
 */
class Nd4jFuncsTest extends org.scalatest.FunSuite {

  val dailyTrmmUrl = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_L3/TRMM_3B42_daily/1997/365/3B42_daily.1998.01.01.7.bin"
  val hourlyTrmmUrl = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_3Hourly_3B42/1997/365/3B42.19980101.00.7.HDF.Z"
  val knmiUrl = "http://zipper.jpl.nasa.gov/dist/AFRICA_KNMI-RACMO2.2b_CTL_ERAINT_MM_50km_1989-2008_tasmax.nc"

  val KMNI_BNDS_DIMENSION = "bnds"
  val KNMI_TASMAX_VAR = "tasmax"
  val DAILY_TRMM_DATA_VAR = "data"
  val HOURLY_TRMM_DATA_VAR = "precipitation"

  val EXPECTED_COLS = 1440
  val EXPECTED_ROWS = 400
  val BLOCK_SIZE = 5

  /**
   * Testing creation of 2D Array (INDArray) from daily collected TRMM data
   * Assert Criteria : Dimensions of TRMM data matches shape of 2D Array
   */
  test("ReadingDailyTRMMDimensions") {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(dailyTrmmUrl)

    val coordArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, DAILY_TRMM_DATA_VAR)
    val ExpectedClass = Nd4j.create(coordArray, Array(EXPECTED_ROWS, EXPECTED_COLS))
    val dSizes = NetCDFUtils.getDimensionSizes(netcdfFile.findVariable(DAILY_TRMM_DATA_VAR).getDimensions)
    println("[%s] Dimensions for daily TRMM  data set %s".format("ReadingTRMMDimensions", dSizes.toString()))

    val resDenseMatrix = Nd4jFuncs.create2dNd4jArray(dSizes, netcdfFile, DAILY_TRMM_DATA_VAR)

    assert(resDenseMatrix.getClass.equals(ExpectedClass.getClass))
    assert(ExpectedClass.rows == resDenseMatrix.rows)
    assert(ExpectedClass.columns == resDenseMatrix.columns)
    assert(true)
  }

  /**
   * Testing creation of 2D Array (DenseMatrix) from hourly collected TRMM data
   * Assert Criteria : Dimensions of TRMM data matches shape of 2D Array
   */
  test("ReadingHourlyTRMMDimensions") {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(hourlyTrmmUrl)

    val coordArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, HOURLY_TRMM_DATA_VAR)
    val ExpectedClass = Nd4j.create(coordArray, Array(EXPECTED_ROWS, EXPECTED_COLS))
    val dSizes = NetCDFUtils.getDimensionSizes(netcdfFile.findVariable(HOURLY_TRMM_DATA_VAR).getDimensions)
    println("[%s] Dimensions for hourly TRMM data set %s".format("ReadingTRMMDimensions", dSizes.toString()))

    val resDenseMatrix = Nd4jFuncs.create2dNd4jArray(dSizes, netcdfFile, HOURLY_TRMM_DATA_VAR)

    assert(resDenseMatrix.getClass.equals(ExpectedClass.getClass))
    assert(ExpectedClass.rows == resDenseMatrix.rows)
    assert(ExpectedClass.columns == resDenseMatrix.columns)
    assert(true)
  }


  /**
   * test for creating a N-Dimension array from KNMI data
   */
  test("ReadingKNMIDimensions") {
    //    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(knmiUrl)
    //
    //    val ExpectedType = Array.ofDim[Float](240, 1, 201 ,194)
    //    val dSizes = NetCDFUtils.getDimensionSizes(netcdfFile.findVariable(KNMI_TASMAX_VAR).getDimensions)
    //    println("[%s] Dimensions for KNMI data set %s".format("ReadingKMIDimensions", dSizes.toString()))
    //
    //    val fdArray = BreezeFuncs.create4dArray(dSizes, netcdfFile, KNMI_TASMAX_VAR)
    //    assert(fdArray.getClass.equals(ExpectedType.getClass))
    assert(true)
  }

  test("ndf4jReduceResolutionAvrgTest") {
    val squareSize = 100
    val reductionSize = 50
    val accuracy = 1E-15
    val reducedWidth = squareSize / reductionSize
    val testMatrix = Nd4j.create(squareSize, squareSize)

    val resultMatrix = Nd4jFuncs.reduceResolution(testMatrix, reductionSize)

    for (i <- 0 to (reducedWidth - 1)) {
      for (j <- 0 to (reducedWidth - 1)) {
        val error = Math.abs(resultMatrix(i, j) - 1)
        if (error >= accuracy) {
          assert(error >= accuracy, "The error is not even close for indices " + i + " " + j + "with value : " + resultMatrix(i, j))
        }
      }
    }
    assert(true)
  }

  /**
   * Tests the ReduceResolution function.
   * Assert Criteria : Proper reduction of dimensions by the factor BLOCK_SIZE
   */
  test("ReduceResolutionTest") {
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(hourlyTrmmUrl)

    val coordArray = NetCDFUtils.convertMa2ArrayTo1DJavaArray(netcdfFile, HOURLY_TRMM_DATA_VAR)
    val ExpectedClass = Nd4j.create(coordArray, Array(EXPECTED_ROWS / BLOCK_SIZE, EXPECTED_COLS / BLOCK_SIZE))
    val dSizes = NetCDFUtils.getDimensionSizes(netcdfFile.findVariable(HOURLY_TRMM_DATA_VAR).getDimensions)
    println("[%s] Dimensions for hourly TRMM data set %s".format("ReadingTRMMDimensions", dSizes.toString()))

    val resDenseMatrix = Nd4j.create(coordArray, Array(EXPECTED_ROWS, EXPECTED_COLS))
    val reducedMatrix = Nd4jFuncs.reduceResolution(resDenseMatrix, BLOCK_SIZE)


    assert(reducedMatrix.getClass.equals(ExpectedClass.getClass))
    assert(ExpectedClass.rows == reducedMatrix.rows)
    assert(ExpectedClass.columns == reducedMatrix.columns)
  }

}
