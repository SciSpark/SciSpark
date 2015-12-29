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

/**
 * Tests whether the creation of Nd4jTensors works.
 */
class Nd4jIntegrationTest extends FunSuite {

  /** URLs */
  val dailyTrmmUrl = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_L3/TRMM_3B42_daily/1997/365/3B42_daily.1998.01.01.7.bin"
  val hourlyTrmmUrl = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_3Hourly_3B42/1997/365/3B42.19980101.00.7.HDF.Z"
  val airslvl3 = "http://acdisc.sci.gsfc.nasa.gov/opendap/ncml/Aqua_AIRS_Level3/AIRH3STD.005/2003/AIRS.2003.01.01.L3.RetStd_H001.v5.0.14.0.G07285113200.hdf.ncml"
  val knmiUrl = "http://zipper.jpl.nasa.gov/dist/AFRICA_KNMI-RACMO2.2b_CTL_ERAINT_MM_50km_1989-2008_tasmax.nc"

  /** variables */
  val KMNI_BNDS_DIMENSION = "bnds"
  val KNMI_TASMAX_VAR = "tasmax"
  val DAILY_TRMM_DATA_VAR = "data"
  val HOURLY_TRMM_DATA_VAR = "precipitation"
  val TOTAL_LIQH20 = "TotCldLiqH2O_A"

  /** expected tensor shapes */
  val EXPECTED_COLS = 400
  val EXPECTED_ROWS = 1440
  val EXPECTED_ROWS_DAILY = 400
  val EXPECTED_COLS_DAILY = 1440

  /**
   * Testing creation of 2D Array from daily collected TRMM compData
   */
  test("ReadingDailyTRMMDimensions") {
    /** creating expected */
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(dailyTrmmUrl)
    val coordArray = NetCDFUtils.netCDFArrayAndShape(netcdfFile, DAILY_TRMM_DATA_VAR)
    val dSizes = coordArray._2.toList
    println("[%s] Dimensions for daily TRMM  data set %s".format("ReadingTRMMDimensions", dSizes.toString()))
    /** creating actual and comparing to expected */
    val realTensor = new Nd4jTensor(NetCDFReader.loadNetCDFNDVar(dailyTrmmUrl, DAILY_TRMM_DATA_VAR))
    assert(realTensor.data.length == (realTensor.shape(0) * realTensor.shape(1)))
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
    println("[%s] Dimensions for hourly TRMM data set %s".format("ReadingTRMMDimensions", dSizes.toString()))
    /** creating actual and comparing to expected */
    val realTensor = new Nd4jTensor(NetCDFReader.loadNetCDFNDVar(hourlyTrmmUrl, HOURLY_TRMM_DATA_VAR))
    assert(realTensor.tensor.getClass.equals(expectedClass.getClass))
    assert(realTensor.shape.toList == expectedClass.shape.toList)
  }

  /**
   * Testing creation of N-d array from KNMI compData
   */
  test("ReadingKNMIDimensions") {
    /** creating expected */
    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(knmiUrl)
    val coordArray = NetCDFUtils.netCDFArrayAndShape(netcdfFile, KNMI_TASMAX_VAR)
    val expectedType = Nd4j.zeros(240, 201, 194)
    val dSizes = coordArray._2.toList
    println("[%s] Dimensions for KNMI data set %s".format("ReadingKMIDimensions", dSizes.toString()))
    /** creating actual and comparing to expected */
    val realTensor = new Nd4jTensor(coordArray)
    assert(realTensor.tensor.getClass.equals(expectedType.getClass))
    assert(realTensor.shape.toList == expectedType.shape.toList)
  }

  /**
   * Testing creation of N-d array from AIRS compData
   */
  test("ReadingAIRSDimensions") {
    val realTensor = new Nd4jTensor(NetCDFReader.loadNetCDFNDVar(airslvl3, TOTAL_LIQH20))
    assert(realTensor.tensor.rows() == 180)
    assert(realTensor.tensor.columns() == 360)
  }

}
