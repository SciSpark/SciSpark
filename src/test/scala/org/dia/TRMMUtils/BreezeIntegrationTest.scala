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
package org.dia.TRMMUtils


import org.dia.loaders.NetCDFReader
import org.dia.tensors.BreezeTensor


/**
 * Tests for the Breeze functions
 */
class BreezeIntegrationTest extends org.scalatest.FunSuite {

  val dailyTrmmUrl = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_L3/TRMM_3B42_daily/1997/365/3B42_daily.1998.01.01.7.bin"
  val hourlyTrmmUrl = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_3Hourly_3B42/1997/365/3B42.19980101.00.7.HDF.Z"
  val knmiUrl = "http://zipper.jpl.nasa.gov/dist/AFRICA_KNMI-RACMO2.2b_CTL_ERAINT_MM_50km_1989-2008_tasmax.nc"

  val KMNI_BNDS_DIMENSION = "bnds"
  val KNMI_TASMAX_VAR = "tasmax"
  val DAILY_TRMM_DATA_VAR = "data"
  val HOURLY_TRMM_DATA_VAR = "precipitation"

  val EXPECTED_COLS_DAILY = 1440
  val EXPECTED_ROWS_DAILY = 400
  val EXPECTED_COLS_HOURLY = 400
  val EXPECTED_ROWS_HOURLY = 1440
  val BLOCK_SIZE = 5

  /**
   * Testing creation of 2D Array (DenseMatrix) from daily collected TRMM data
   * Note that Breeze by default uses fortran column major ordering.
   * We check specifically that BreezeTensor flips this and uses row major ordering.
   * Notice the flipped parameters for the DenseMatrix constructor.
   * Assert Criteria : Dimensions of TRMM data matches shape of 2D Array
   */
  test("ReadingDailyTRMMDimensions") {
    val arrayTuple = NetCDFReader.loadNetCDFNDVars(dailyTrmmUrl, DAILY_TRMM_DATA_VAR)
    val resDenseMatrix = new BreezeTensor(arrayTuple)
    assert(EXPECTED_ROWS_DAILY == resDenseMatrix.rows)
    assert(EXPECTED_COLS_DAILY == resDenseMatrix.cols)
    assert(true)
  }


  /**
   * Testing creation of 2D Array (DenseMatrix) from hourly collected TRMM data
   * Assert Criteria : Dimensions of TRMM data matches shape of 2D Array
   */
  test("ReadingHourlyTRMMDimensions") {
    val arrayTuple = NetCDFReader.loadNetCDFNDVars(hourlyTrmmUrl, HOURLY_TRMM_DATA_VAR)
    val resDenseMatrix = new BreezeTensor(arrayTuple)

    assert(EXPECTED_ROWS_HOURLY == resDenseMatrix.rows)
    assert(EXPECTED_COLS_HOURLY == resDenseMatrix.cols)
    assert(true)
  }


  /**
   * test for creating a N-Dimension array from KNMI data
   * This test is ignored due to the massive data set size.
   * It is a 7k x 3k x 60 array.
   */
  test("ReadingKNMIDimensions") {
    val arrayTuple = NetCDFReader.loadNetCDFNDVars(knmiUrl, KNMI_TASMAX_VAR)
    val resDenseMatrix = new BreezeTensor(arrayTuple)

    val ExpectedShape = arrayTuple._2
    val BreezeShapeLimit = Array(ExpectedShape(0), ExpectedShape(1)).toList
    println("[%s] Dimensions for KNMI data set %s".format("ReadingKMIDimensions", ExpectedShape.toList.toString()))
    assert(resDenseMatrix.shape.toList.equals(BreezeShapeLimit))
  }


}
