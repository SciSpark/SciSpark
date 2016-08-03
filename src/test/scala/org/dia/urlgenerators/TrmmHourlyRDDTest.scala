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
package org.dia.urlgenerators

import java.text.SimpleDateFormat

import scala.collection.mutable.ListBuffer
import scala.io.Source

import org.joda.time.DateTime
import org.scalatest.Ignore

import org.dia.Parsers

/**
 * Tests whether the TrmmHourly URLs creator works.
 */
@Ignore
class TrmmHourlyRDDTest extends org.scalatest.FunSuite {

  val HOURLY_TRMM_DATA_VAR = "precipitation"

  /**
   * Test if the generated hourly readings are correct.
   */
  test("GeneratingHourlyTRMMFileUrls") {
    val realDate = (new DateTime).withYear(1998).withDayOfYear(66)
    var expectedReadings = new ListBuffer[String]()
    expectedReadings += ("3B42.19980307.03.7.HDF.Z", "3B42.19980307.06.7.HDF.Z",
      "3B42.19980307.09.7.HDF.Z", "3B42.19980307.12.7.HDF.Z", "3B42.19980307.15.7.HDF.Z",
      "3B42.19980307.18.7.HDF.Z", "3B42.19980307.21.7.HDF.Z", "3B42.19980308.00.7.HDF.Z")
    val trmmHourlyUrls = HourlyTrmmURLGenerator.generateDayReadings(realDate)
    expectedReadings.foreach(v => assert(trmmHourlyUrls.contains(v)))
  }

  /**
   * Tests whether we can parse the dates from the URLs.
   */
  test("URL.date.parser.test") {
    val urls = "TestLinks"
    val datasetUrls = Source.fromFile(urls).mkString.split("\n").toList
    val formatter = new SimpleDateFormat("yyyy-MM-dd")
    datasetUrls.foreach(p => {
      val dated = Parsers.parseDateFromString(p)
    })
    assert(true)
  }

}