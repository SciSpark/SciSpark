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
package org.dia.integration

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.dia.loaders.NetCDFReader
import org.dia.tensors.BreezeTensor
import org.dia.utils.NetCDFUtils

/**
 * Tests whether the creation of BreezeTensors works.
 * Note that a likely reason for these tests to fail
 * Is if a filename has changed on the OpenDap server where these
 * files are being pulled from.
 *
 * To check, simply visit the url corresponding to the failed test.
 */
class BreezeIntegrationTest extends FunSuite with BeforeAndAfter{

  /**
   * Earth Data login for GESDISC : Authentication
   * https://urs.earthdata.nasa.gov/home
   * You can log in with the following credentials:
   * Username : scispark
   * Password : SciSpark1
   */
  val username = "scispark"
  val password = "SciSpark1"
  /** URLs */
  val gesdiscUrl = "http://disc2.gesdisc.eosdis.nasa.gov:80"
  val airsUrl = "http://acdisc-ts1.gesdisc.eosdis.nasa.gov:80"
  val dailyTrmmUrl = gesdiscUrl + "/opendap/hyrax/TRMM_L3/TRMM_3B42_Daily.7/1998/01/3B42_Daily.19980101.7.nc4"
  val hourlyTrmmUrl = gesdiscUrl + "/opendap/hyrax/TRMM_L3/TRMM_3B42/1997/365/3B42.19980101.00.7.HDF"
  val knmiUrl = "http://zipper.jpl.nasa.gov/dist/AFRICA_KNMI-RACMO2.2b_CTL_ERAINT_MM_50km_1989-2008_tasmax.nc"

  val KMNI_BNDS_DIMENSION = "bnds"
  val KNMI_TASMAX_VAR = "tasmax"
  val DAILY_TRMM_DATA_VAR = "precipitation"
  val HOURLY_TRMM_DATA_VAR = "precipitation"

  val EXPECTED_COLS_DAILY = 400
  val EXPECTED_ROWS_DAILY = 1440
  val EXPECTED_COLS_HOURLY = 400
  val EXPECTED_ROWS_HOURLY = 1440
  val BLOCK_SIZE = 5

  before {
    NetCDFUtils.setHTTPAuthentication(gesdiscUrl, username, password)
    NetCDFUtils.setHTTPAuthentication(airsUrl, username, password)
  }
  /**
   * Testing creation of 2D Array (DenseMatrix) from daily collected TRMM data.
   * Note that Breeze by default uses Fortran column major ordering.
   * We check specifically that BreezeTensor flips this and uses row major ordering.
   * Notice the flipped parameters for the DenseMatrix constructor.
   * Assert Criteria : Dimensions of TRMM data matches shape of 2D Array
   */
  test("ReadingDailyTRMMDimensions") {
    val arrayTuple = NetCDFReader.loadNetCDFNDVar(dailyTrmmUrl, DAILY_TRMM_DATA_VAR)
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
    val arrayTuple = NetCDFReader.loadNetCDFNDVar(hourlyTrmmUrl, HOURLY_TRMM_DATA_VAR)
    val resDenseMatrix = new BreezeTensor(arrayTuple)
    assert(EXPECTED_ROWS_HOURLY == resDenseMatrix.rows)
    assert(EXPECTED_COLS_HOURLY == resDenseMatrix.cols)
    assert(true)
  }

  /**
   * Testing creation of an N-dimensional array from KNMI data.
   * This test is ignored due to the massive size of the data set.
   * It is a 7k x 3k x 60 array.
   */
  ignore("ReadingKNMIDimensions") {
    val arrayTuple = NetCDFReader.loadNetCDFNDVar(knmiUrl, KNMI_TASMAX_VAR)
    val resDenseMatrix = new BreezeTensor(arrayTuple)
    val expectedShape = arrayTuple._2
    val breezeShapeLimit = Array(expectedShape(0), expectedShape(1)).toList
    println("[%s] Dimensions for KNMI data set %s".format("ReadingKMIDimensions", expectedShape.toList.toString()))
    assert(resDenseMatrix.shape.toList.equals(breezeShapeLimit))
  }

}
