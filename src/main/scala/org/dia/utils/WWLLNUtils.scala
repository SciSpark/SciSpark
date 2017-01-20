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
package org.dia.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

import org.dia.core.{SciSparkContext}

/**
 * Utilities for working with WWLLN data in SciSpark
 */

object WWLLNUtils extends Serializable {

  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Creates a column of datetime string in SimpleDateFormat("yyyy/MM/DD kk:mm:ss")
   * from the first two columns of WWLLN data
   * @param datePart The data in column 1 that represents the date
   * @param timePart The data in column 2 that represents the time to microseconds
   * @return string in SimpleDateFormat("yyyy/MM/DD kk:mm:ss")
   */
  def getWWLLNtimeStr(datePart: String, timePart: String): String = {
    if (datePart.toString() == "") {
      return null
    } else {
      return datePart + " " + timePart.split('.').dropRight(1).mkString("")
    }
  }

  /**
   * Utility function to convert dateTimeStr to Timestamp object.
   * @param currTimeStr The string to convert to the timestamp
   * @return timestamp object of input data
   */
  def getWWLLNDateTime(currTimeStr: String): Timestamp = {
    val format = new SimpleDateFormat("yyyy/MM/DD kk:mm:ss")
    if (currTimeStr.toString() == "") {
      return null
    } else {
      val d = format.parse(currTimeStr.toString())
      val t = new Timestamp(d.getTime())
      return t
    }
  }

  /**
   * Queries the WWLLN data for lightning strike at a given time within a region-box
   * @param WWLLN The broadcasted WWLLN data stored within a dataframe
   * @param thisTimeDate The timeDate string to check in the format "yyyy/MM/DD kk:mm:ss"
   * @param minLat The minimum latitude within the region-box to check for lightning strike
   * @param maxLat The maximum latitude within the region-box to check for lightning strike
   * @param minLon The minimum longitude within the region-box to check for lightning stirke
   * @param maxLon The maximum longitude within the region-box to check for lightning strike
   * @return Results of the query to an Array of tuples in the format (Double, Double). If value
   *         is given as Array() then no lightning strikes were found.
   */
  def getLightningLocs(
      WWLLN: Broadcast[DataFrame],
      thisTimeDate: String,
      minLat: Double,
      maxLat: Double,
      minLon: Double,
      maxLon: Double): Array[(Double, Double)] = {
    val filterCmd = "(lat > " + minLat.toString + " and lat < " + maxLat.toString +
      ") and (lon > " + minLon.toString + " and lon < " + maxLon.toString +
      ") and (lDateTime = '" + thisTimeDate + "')"
    val result = WWLLN.value.filter(filterCmd)
    result.rdd.map( r => (r(1).asInstanceOf[Double], r(2).asInstanceOf[Double])).collect()
  }

}
