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

import org.dia.Constants.{ TRMM_HOURLY_DATA_PREFFIX, TRMM_HOURLY_DATA_SUFFIX, TRMM_HOURLY_URL }
import org.joda.time.DateTime
import scala.collection.mutable.ListBuffer

/**
 * Generates hourly TRMM URL's.
 *
 * The function is used more as a staging function to generate TRMM URL's.
 * It does not necessarily have to be part of the application pipeline.
 */
object HourlyTrmmURLGenerator {

  /**
   * Generates the readings between two years.
   */
  def generateTrmmDaily(iniYear: Int, finalYear: Int = 0) = {
    val yearReadings = new ListBuffer[String]()
    val maxDays = 2
    /** only a single year */
    if (finalYear == 0) {
      for (day <- 1 to maxDays) {
        val realDate = (new DateTime).withYear(iniYear).withDayOfYear(day)
        yearReadings.appendAll(generateDayReadings(realDate))
      }
    } else {
      /** a range of years */
      for (iYear <- iniYear to finalYear by 1) {
        for (day <- 1 to maxDays) {
          val realDate = (new DateTime).withYear(iYear).withDayOfYear(day)
          yearReadings.appendAll(generateDayReadings(realDate))
        }
      }
    }
    yearReadings
  }

  /**
   * Generating readings for a specific day.
   */
  def generateDayReadings(realDate: DateTime) = {
    var dailyReadings = new ListBuffer[String]()
    val sb = new StringBuilder
    for (reading <- 3 to 24 by 3) {
      sb.append(TRMM_HOURLY_URL).append(realDate.getYear.toString).append("/")
      sb.append("%03d".format(realDate.getDayOfYear)).append("/")
      sb.append(TRMM_HOURLY_DATA_PREFFIX).append(".")
      if (reading != 24) {
        sb.append("%s".format(realDate.toString("yyyyMMdd"))).append(".")
        sb.append("%02d".format(reading))
      } else {
        sb.append("%s".format(realDate.plusDays(1).toString("yyyyMMdd"))).append(".")
        sb.append("00")
      }
      sb.append(TRMM_HOURLY_DATA_SUFFIX)
      dailyReadings += sb.toString()
      sb.clear()
    }
    dailyReadings
  }

}
