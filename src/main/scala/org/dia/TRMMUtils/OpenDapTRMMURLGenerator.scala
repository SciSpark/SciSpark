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

import java.io.{File, PrintWriter}
import java.net.{HttpURLConnection, URL}
import java.util

import org.joda.time.DateTime

import scala.collection.JavaConversions._

/**
 * Generates a list of links for the TRMM tensors
 */
object OpenDapTRMMURLGenerator {
  val URL = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_L3/TRMM_3B42_daily/"
  val DEFAULT_FILE_NAME = "TRMM_L3_Links.txt"
  val iniYear = 1997
  val endYear = 2015
  var fileName = ""
  var checkUrl = false

  def run(checkLink: Boolean): Unit = {
    run(checkLink, DEFAULT_FILE_NAME)
  }

  /**
   * Runs the OpenDapTRMMURL link generator
   * @param checkLink if the link needs to bec checked ?Deprecated?
   */
  def run(checkLink: Boolean, fName: String): Unit = {
    // initializing variables
    checkUrl = checkLink
    fileName = fName
    val numYears = (0 to (endYear - iniYear)).toList
    // reading time
    var readTime = new DateTime(1997, 1, 2, 0, 0)
    val pw = new PrintWriter(new File(fileName))
    pw.flush()
    val totalUrls = new util.ArrayList[String]()
    try {
      numYears.par.foreach { e => totalUrls.addAll(generateLinksPerYear(e)) }
      println("Total URLs: " + totalUrls.size())
      totalUrls.foreach { e => pw.append(e.toString + "\n") }
    } catch {
      case ex: Exception =>
        println("Exception")
        ex.printStackTrace()
    } finally {
      pw.close()
    }
  }

  /**
   * Gets the links per year
   * @param year the year offset 1997 e.g. For Starting at 2000 -> year = 3
   * @return
   */
  def generateLinksPerYear(year: Int): util.ArrayList[String] = {

    val checkedYear = iniYear + year
    val urls = new util.ArrayList[String]()
    val days = if (checkedYear % 4 == 0) 366 else 365
    var readTime = new DateTime(checkedYear, 1, 2, 0, 0)
    //for each year try to generate each day
    for (day <- 1 to days) {
      val paddedDay = (day.toString.reverse + "00").substring(0, 3).reverse
      val paddedMonth = (readTime.getMonthOfYear.toString.reverse + "0").substring(0, 2).reverse
      val paddedReadDay = (readTime.getDayOfMonth.toString.reverse + "0").substring(0, 2).reverse
      readTime = readTime.plusDays(1)

      val sb = new StringBuilder()
      sb.append(checkedYear).append("/")
      sb.append(paddedDay).append("/")
      sb.append("3B42_daily.").append(readTime.getYear).append(".")
      sb.append(paddedMonth).append(".")
      sb.append(paddedReadDay).append(".7.bin")
      // check url and stop if it doesn't exist
      val tmpUrl = URL + sb.toString
      if (checkUrl) {
        if (getResponseCode(tmpUrl)) {
          urls.add(tmpUrl)
          //println(tmpUrl)
        }
      }
    }
    urls
  }

  /**
   * Checks if the url actually exists
   * @param urlString the url
   * @return
   */
  def getResponseCode(urlString: String): Boolean = {
    val u = new URL(urlString)
    val huc = u.openConnection().asInstanceOf[HttpURLConnection]
    huc.setConnectTimeout(100000)
    huc.setRequestMethod("HEAD")
    huc.getResponseCode == HttpURLConnection.HTTP_OK
  }

}
