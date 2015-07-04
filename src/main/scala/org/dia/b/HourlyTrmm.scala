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

import org.dia.Constants.TRMM_HOURLY_DATA_PREFFIX
import org.dia.Constants.TRMM_HOURLY_DATA_SUFFIX
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer

/**
 * Container of HourlyTrmm data
 */
object HourlyTrmm {

  def loadTrmmDaily(datasetUrl: String, iniYear: Int, finalYear: Int) = {
//    val maxDays = if (iniYear%4 == 0) 366 else 355
    val maxDays = 1
    var yearReadings = new ListBuffer[String]()
    for (day <- 1 to maxDays) {
      val realDate = (new DateTime).withYear(iniYear).withDayOfYear(day)
      yearReadings.appendAll(generateDayReadings(realDate))
      println(yearReadings)
    }
    yearReadings
  }

  def generateDayReadings(realDate: DateTime) = {
    val sb = new StringBuilder
    var dailyReadings = new ListBuffer[String]()
    for (reading <- 3 to 24 by 3) {
      sb.append(TRMM_HOURLY_DATA_PREFFIX).append(".")
      if (reading != 24) {
        sb.append("%s".format(realDate.toString("yyyyMMdd"))).append(".")
        sb.append(if (reading >= 10) reading else "0%d".format(reading))
      }
      else {
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
