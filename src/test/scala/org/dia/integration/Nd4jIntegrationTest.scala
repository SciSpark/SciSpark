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

import org.dia.utils.NetCDFUtils
import org.dia.loaders.NetCDFReader
import org.dia.tensors.Nd4jTensor
import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter


/**
 * Tests whether the creation of Nd4jTensors works.
 * Note that a likely reason for these tests to fail
 * Is if a filename has changed on the OpenDap server where these
 * files are being pulled from.
 *
 * To check, simply visit the url corresponding to the failed test.
 */
class Nd4jIntegrationTest extends FunSuite with BeforeAndAfter {

  /**
    * Earth Data login for GESDISC : Authentication
    * https://urs.earthdata.nasa.gov/home
    * You can log in with the following credentials:
    * Username : scispark
    * Password : SciSpark1
    * */
  val username = "scispark"
  val password = "SciSpark1"
  /** URLs */
  val gesdiscUrl = "http://disc2.gesdisc.eosdis.nasa.gov:80"
  val airsUrl = "http://acdisc-ts1.gesdisc.eosdis.nasa.gov:80"
  val airsHDFFile = "AIRS.2003.01.01.L3.RetStd_H001.v6.0.12.0.G14112110740.hdf"
  val dailyTrmmUrl = gesdiscUrl + "/opendap/hyrax/TRMM_L3/TRMM_3B42_Daily.7/1998/01/3B42_Daily.19980101.7.nc4"
  val hourlyTrmmUrl = gesdiscUrl + "/opendap/hyrax/TRMM_L3/TRMM_3B42/1997/365/3B42.19980101.00.7.HDF"
  val airslvl3 = airsUrl + "/opendap/urs/Aqua_AIRS_Level3/AIRH3STD.006/2003/" + airsHDFFile
  val knmiUrl = "http://zipper.jpl.nasa.gov/dist/AFRICA_KNMI-RACMO2.2b_CTL_ERAINT_MM_50km_1989-2008_tasmax.nc"

  /** variables */
  val KMNI_BNDS_DIMENSION = "bnds"
  val KNMI_TASMAX_VAR = "tasmax"
  val DAILY_TRMM_DATA_VAR = "precipitation"
  val HOURLY_TRMM_DATA_VAR = "precipitation"
  val TOTALCOUNTS_A = "TotalCounts_A"

  /** expected tensor shapes */
  val EXPECTED_COLS = 400
  val EXPECTED_ROWS = 1440
  val EXPECTED_COLS_DAILY = 400
  val EXPECTED_ROWS_DAILY = 1440

  before {
    NetCDFUtils.setHTTPAuthentication(gesdiscUrl, username, password)
    NetCDFUtils.setHTTPAuthentication(airsUrl, username, password)
  }

  /**
   * Testing creation of 2D Array from daily collected TRMM compData
   */
  test("ReadingDailyTRMMDimensions") {
    /** creating expected */
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(dailyTrmmUrl)
    val coordArray = NetCDFUtils.netCDFArrayAndShape(netcdfFile, DAILY_TRMM_DATA_VAR)
    val dSizes = coordArray._2.toList
    /** creating actual and comparing to expected */
    val realTensor = new Nd4jTensor(NetCDFReader.loadNetCDFNDVar(dailyTrmmUrl, DAILY_TRMM_DATA_VAR))
    assert(realTensor.data.length == realTensor.shape.product)
    assert(realTensor.tensor.columns == EXPECTED_COLS_DAILY)
    assert(realTensor.tensor.rows == EXPECTED_ROWS_DAILY)
  }

  /**
   * Testing creation of 2D Array from hourly collected TRMM compData
   */
  test("ReadingHourlyTRMMDimensions") {
    /** creating expected */
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(hourlyTrmmUrl)
    val coordArray = NetCDFUtils.netCDFArrayAndShape(netcdfFile, HOURLY_TRMM_DATA_VAR)
    val expectedClass = Nd4j.create(coordArray._1, Array(EXPECTED_ROWS, EXPECTED_COLS))
    val dSizes = coordArray._2.toList
    /** creating actual and comparing to expected */
    val realTensor = new Nd4jTensor(NetCDFReader.loadNetCDFNDVar(hourlyTrmmUrl, HOURLY_TRMM_DATA_VAR))
    assert(realTensor.tensor.getClass.equals(expectedClass.getClass))
    assert(realTensor.shape.toList == expectedClass.shape.toList)
  }

  /**
   * Testing creation of N-d array from AIRS compData
   */
  test("ReadingAIRSDimensions") {
    val realTensor = new Nd4jTensor(NetCDFReader.loadNetCDFNDVar(airslvl3, TOTALCOUNTS_A))
    assert(realTensor.tensor.rows() == 180)
    assert(realTensor.tensor.columns() == 360)
  }


  /**
    * Testing creation of N-d array from KNMI compData
    * This test is being ignored due to the massive size of the dataset
    * It is a 7k x 3k x 60 array
    */
  ignore("ReadingKNMIDimensions") {
    /** creating expected */
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(knmiUrl)
    val coordArray = NetCDFUtils.netCDFArrayAndShape(netcdfFile, KNMI_TASMAX_VAR)
    val expectedType = Nd4j.zeros(240, 201, 194)
    val dSizes = coordArray._2.toList
    /** creating actual and comparing to expected */
    val realTensor = new Nd4jTensor(coordArray)
    assert(realTensor.tensor.getClass.equals(expectedType.getClass))
    assert(realTensor.shape.toList == expectedType.shape.toList)
  }
}
