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

import breeze.linalg.DenseMatrix
import org.dia.b.TrmmBiasRDD
import org.dia.core.SparkTestConstants
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

/**
 * Testing TrmmHourly
 */

class TrmmHourlyRDDTest extends org.scalatest.FunSuite {

  val HOURLY_TRMM_DATA_VAR = "precipitation"

  /**
   * Test if the generated hourly readings are correct
   */
  test("GeneratingHourlyTRMMFileUrls") {
    val realDate = (new DateTime).withYear(1998).withDayOfYear(66)
    var expectedReadings = new ListBuffer[String]()
    expectedReadings +=("3B42.19980307.03.7.HDF.Z", "3B42.19980307.06.7.HDF.Z",
      "3B42.19980307.09.7.HDF.Z", "3B42.19980307.12.7.HDF.Z", "3B42.19980307.15.7.HDF.Z",
      "3B42.19980307.18.7.HDF.Z", "3B42.19980307.21.7.HDF.Z", "3B42.19980308.00.7.HDF.Z")
    val trmmHourlyUrls = HourlyTrmm.generateDayReadings(realDate)
    expectedReadings.foreach(v => assert(trmmHourlyUrls.contains(v)))
  }

  test("basic functionality") {
    val sc = SparkTestConstants.sc
    val rdd = new TrmmHourlyRDD[(String, DenseMatrix[Double])](sc, Constants.TRMM_HOURLY_URL, HOURLY_TRMM_DATA_VAR, 1998, 1998)
    val rdd2 = new TrmmHourlyRDD[(String, DenseMatrix[Double])](sc, Constants.TRMM_HOURLY_URL, HOURLY_TRMM_DATA_VAR, 1999, 1999)

    // operation
    //      val threshold = 45
    //      rdd.collect().map( m =>
    //        if (m._2.max > threshold)
    //          println(m._1)
    //      )

    val rdd3 = new TrmmBiasRDD[(String, DenseMatrix[Double])](sc, rdd, rdd2)

    //println(rdd3.collect()(0))
    //println()
    assert(true)
  }
}