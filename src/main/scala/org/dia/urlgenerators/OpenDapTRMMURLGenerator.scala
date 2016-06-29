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

import java.io.{ File, PrintWriter }
import java.net.{ HttpURLConnection, URL }
import java.util
import scala.collection.JavaConversions._
import org.joda.time.DateTime
import org.joda.time.Days
import org.joda.time.format.DateTimeFormat
import scala.util.control.Breaks._

/**
 * Generates a list of links for the TRMM URLs.
 */
object OpenDapTRMMURLGenerator {

  val URL = "http://disc2.nascom.nasa.gov/opendap/hyrax/TRMM_L3/"//"http://disc2.nascom.nasa.gov:80/opendap/TRMM_L3/" 
  val DEFAULT_FILE_NAME = "Test_TRMM_L3_Links.txt"
  var fileName = ""
  var checkUrl = false
  var startTime = ""
  var endTime = ""
  var tRes = 0

  // def run(checkLink: Boolean): Unit = {
  //   run(checkLink, DEFAULT_FILE_NAME)
  // }

  /**
   * Runs the OpenDapTRMMURL link generator
   * 
   * @param checkLink if the link needs to be checked, deprecated?
   * @param fName file to write URLs into
   * @param startTime starttime in the format YYYYMMDDHHmm
   * @param endTime endtime in the format YYYYMMDDHHmm
   * @param tRes temporal resolution for the files. Options are 1-daily or 2-3hrly
   */
  def run(checkLink: Boolean, fName: String, starttime: String, endtime: String, tres:Int, varNames:List[String]): Unit = {
    /** initializing variables */
    checkUrl = checkLink
    fileName = fName
    startTime = starttime
    endTime = endtime
    tRes = tres

    if (startTime.length != 12){
      println("startTime is an incorrect format. Using defaults value of 199701020000")
      startTime = "199701020000"
    }
    if (endTime.length != 12){
      println("endTime is an incorrect format. Using default value of 201501020000")
      endTime = "201501020000"
    }

    if (tRes < 1 || tRes > 2){
      println("invalid temporal resolution for TRMM data. Using default value of 2-daily.")
      tRes = 2
    }

    val pw = new PrintWriter(new File(fileName))
    println(pw)
    pw.flush()
    var totalUrls = new util.ArrayList[String]()

    try {
      if (tRes == 1){
        // ensure the HH and mm options are 00
        startTime = startTime.substring(0,8) + "0000"
        endTime = endTime.substring(0,8) + "0000" 
        totalUrls.addAll(generateDailyLinks(startTime, endTime, varNames))
        }else if (tRes == 2){
          val thisURL = URL + "TRMM_3B42/"  
          // ensure the mm option is 00
          startTime = startTime.substring(0,10) + "00"
          endTime = endTime.substring(0,10) + "00"
          totalUrls.addAll(generate3HrlyLinks(startTime, endTime, varNames))
        }
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
   * Gets the num of days and current time to use.
   * 
   * @param currYear An Int representing the current year
   * @param startTime A 12- character String in the format YYYYMMDDHHmm representing the startTime
   * @param endTime A 12- character String in the format YYYYMMDDHHmm representing the endTime
   * @return (startday, days, currTime) A tuple of startday of the yr, num of days as an Int and the current date as a joda.time DateTime object
   */
  def numOfDaysAndDate(currYear:Int, startTime:String, endTime:String) = {
    val sTime = sectionTime(startTime)
    val eTime = sectionTime(endTime)
    var days = 0
    var currTime = new DateTime()
    var newYear = new DateTime()
    var startday = 1

    // determine the number of days for this yr
    if (currYear == startTime.substring(0,4).toInt){
      currTime = new DateTime(sTime._1, sTime._2, sTime._3, sTime._4, sTime._5)
      newYear = if (currYear == endTime.substring(0,4).toInt) new DateTime(eTime._1, eTime._2, eTime._3, eTime._4, eTime._5)
        else new DateTime(currYear+1, 1, 1, 0, 0) 
      startday = if (currTime.getDayOfYear() != 1) currTime.getDayOfYear() - 1 else 1
      days =  Days.daysBetween(currTime, newYear).getDays + 1
    }else if (currYear == endTime.substring(0,4).toInt){
      currTime = new DateTime(eTime._1, eTime._2, eTime._3, eTime._4, eTime._5)
      newYear = new DateTime(currYear, 1, 1, 0, 0) 
      startday = 1
      days =  Days.daysBetween(newYear, currTime).getDays + 1
      currTime = new DateTime(currYear, 1, 1, 0, 0) 
    }else{
      days = if (currYear % 4 == 0) 366 else 365
      currTime = new DateTime(currYear, 1, 1, 0, 0) 
      startday = 1
    }
    (startday, days, currTime)
  }


  /**
   * Gets the sections of the date time as Ints.
   * 
   * @param aTime A 12- character String in the format YYYYMMDDHHmm 
   * @return A tuple of Ints Each component represent a part of the date 
   */
   def sectionTime(aTime: String):(Int, Int, Int, Int, Int) = {
    (aTime.substring(0,4).toInt, aTime.substring(4,6).toInt, aTime.substring(6,8).toInt,
      aTime.substring(8,10).toInt, aTime.substring(10,12).toInt)
  }

  /**
   * Gets the daily links between a time period.
   * 
   * @param startTime A 12- character String in the format YYYYMMDDHHmm representing the startTime
   * @param endTime A 12- character String in the format YYYYMMDDHHmm representing the endTime
   * @param varNames a list of strings entered for the variable and the dimensions to extract from opendap. 
   *        The dim order of the string is "varname, lonminindex, lonmaxindex, latminindex, latmaxindex" 
   *        lonminindex, lonmaxindex, latminindex, latmaxindex are all int values. Using -1 indicates not to subset the dataset.
   * @return URLs List of URLs from the OPeNDaP server for the period
   */
  def generateDailyLinks(startTime:String, endTime:String, varNames:List[String]): util.ArrayList[String] = { 
    
    val numYears = (0 to (endTime.substring(0,4).toInt - startTime.substring(0,4).toInt)).toList
    var urls = new util.ArrayList[String]()
      
    for (year <- numYears){
      val currYear = startTime.substring(0,4).toInt + year
      var (startday, days, currTime) = numOfDaysAndDate(currYear, startTime, endTime)
      
      // add the data for the days
      for (day <- 1 to days) {
        val paddedMonth = (currTime.getMonthOfYear.toString.reverse + "0").substring(0, 2).reverse
        val paddedReadDay = (currTime.getDayOfMonth.toString.reverse + "0").substring(0, 2).reverse
        var paddedDay = ""
        val sb = new StringBuilder()
        if (startday == 1 && day == 1){
          paddedDay = if ((currYear -1) %4 == 0) "366" else "365"    
          sb.append(currYear-1).append("/")
          sb.append(paddedDay).append("/")
          sb.append("3B42_daily.").append(currTime.getYear).append(".")
          sb.append(paddedMonth).append(".")
          sb.append("01").append(".7.bin?")
          startday = 0
        }else{        
          paddedDay = ((startday + day - 1).toString.reverse + "00").substring(0, 3).reverse
          sb.append(currYear).append("/")
          sb.append(paddedDay).append("/")
          sb.append("3B42_daily.").append(currTime.getYear).append(".")
          sb.append(paddedMonth).append(".")
          sb.append(paddedReadDay).append(".7.bin?")
        }
        val toCheckUrl = URL + "TRMM_3B42_daily/" + sb.dropRight(1).toString + ".html"
        // check varNames
        varNames.foreach(y => {
          val z = y.split(",")
          if (z.length > 1){
            sb.append(z(0))
            for (i <- 1 to z.length-1){
              if (z(i).toInt != -1 && i%2 == 1){
                sb.append("[0:"+z(i)+":")
              }else if (z(i).toInt != -1 && i%2 == 0){
                sb.append(z(i)+"]")
              }else if (z(i).toInt == -1){
                break
              }
            } 
          }else{
            sb.append(y)
          }
          sb.append(",")
        })

        /** check url */  
        val tmpUrl = URL + "TRMM_3B42_daily/" + sb.dropRight(1).toString
        // /** check url */
        if (checkUrl) {
          try{
            if (urlExists(toCheckUrl)) {urls.add(tmpUrl)}
          }catch {
            case ex: Exception =>
              println("Problem with this link")
          }  
        } else { 
          urls.add(tmpUrl)
        }

        currTime = currTime.plusDays(1)
      }
    }
  urls
  }

  /**
   * Gets the 3-hrly links between a time period.
   * 
   * @param startTime A 12- character String in the format YYYYMMDDHHmm representing the startTime
   * @param endTime A 12- character String in the format YYYYMMDDHHmm representing the endTime
   * @param varNames a list of strings entered for the variable and the dimensions to extract from opendap. 
   *        The dim order of the string is "varname, lonminindex, lonmaxindex, latminindex, latmaxindex" 
   *        lonminindex, lonmaxindex, latminindex, latmaxindex are all int values. Using -1 indicates not to subset the dataset.
   * @return URLs List of URLs from the OPeNDaP server for the period
   */
  def generate3HrlyLinks(startTime:String, endTime:String, varNames:List[String]): util.ArrayList[String] = { 

    val numYears = (0 to (endTime.substring(0,4).toInt - startTime.substring(0,4).toInt)).toList
    var urls = new util.ArrayList[String]()
    
    for (year <- numYears){
      val currYear = startTime.substring(0,4).toInt + year
      var (startday, days, currTime) = numOfDaysAndDate(currYear, startTime, endTime)
      // add the data for the days
      for (day <- 1 to days) {
        var paddedDay = ((startday + day - 1).toString.reverse + "00").substring(0, 3).reverse
        val paddedMonth = (currTime.getMonthOfYear.toString.reverse + "0").substring(0, 2).reverse
        val paddedReadDay = (currTime.getDayOfMonth.toString.reverse + "0").substring(0, 2).reverse
        var numHrs = (0 to 21 by 3).toList
        if (startday == 1 && day == 1){
          paddedDay = if ((currYear -1) %4 == 0) "366" else "365"
          startday = 0
        }
        
        if (currYear == endTime.substring(0,4).toInt && currTime.getMonthOfYear == endTime.substring(4,6).toInt && currTime.getDayOfMonth == endTime.substring(6,8).toInt){   
          numHrs = if (currYear == startTime.substring(0,4).toInt && currTime.getMonthOfYear == startTime.substring(4,6).toInt && currTime.getDayOfMonth == startTime.substring(6,8).toInt)
            (startTime.substring(8,10).toInt to endTime.substring(8,10).toInt by 3).toList else (0 to endTime.substring(8,10).toInt by 3).toList        
        }else if (currYear == startTime.substring(0,4).toInt && currTime.getMonthOfYear == startTime.substring(4,6).toInt && currTime.getDayOfMonth == startTime.substring(6,8).toInt){
          numHrs = (startTime.substring(8,10).toInt to 21 by 3).toList
        }

        for (hr <- numHrs){
          var paddedHrs = if (hr.toString.length == 1) (hr.toString + "0").reverse else hr.toString
          if (hr != 0){ paddedDay = ((startday + day).toString.reverse + "00").substring(0, 3).reverse}
          val sb = new StringBuilder()
          if (startday == 0 && day == 1 && hr == 0){
            sb.append(currYear-1).append("/")
          }else{
            sb.append(currYear).append("/")
          }
          sb.append(paddedDay).append("/")
          sb.append("3B42.").append(currTime.getYear).append(paddedMonth).append(paddedReadDay).append(".")
          if (currTime.getYear < 2011){
            sb.append(paddedHrs).append(".7A.HDF.Z?")
          }else{
            sb.append(paddedHrs).append(".7.HDF.Z?")
          }
          val toCheckUrl = URL + "TRMM_3B42/" + sb.dropRight(1).toString + ".html"
          // check varNames
          varNames.foreach(y => {
            val z = y.split(",")
            if (z.length > 1){
              sb.append(z(0))
              for (i <- 1 to z.length-1){
                if (z(i).toInt != -1 && i%2 == 1){
                  sb.append("[0:"+z(i)+":")
                }else if (z(i).toInt != -1 && i%2 == 0){
                  sb.append(z(i)+"]")
                }else if (z(i).toInt == -1){
                  break
                }
              } 
            }else{
              sb.append(y)
            }
            sb.append(",")
          })

          /** check url */  
          val tmpUrl = URL + "TRMM_3B42/" + sb.dropRight(1).toString
          // urls.add(tmpUrl)
          if (checkUrl) {
            try{
              if (urlExists(toCheckUrl)) {urls.add(tmpUrl)}
            }catch {
              case ex: Exception =>
                println("Problem with this link")
            }  
          } else { 
            urls.add(tmpUrl)
          }
          currTime = currTime.plusHours(3)
        }     
      }
    }
  urls
  }

  /**
   * Checks if the URL actually exists
   * by sending an HTTP HEAD request.
   * 
   * @param urlString the URL
   * @return boolean whether URL exists.
   */
  def urlExists(urlString: String): Boolean = {
    val u = new URL(urlString)
    val huc = u.openConnection().asInstanceOf[HttpURLConnection]
    huc.setConnectTimeout(100000)
    huc.setRequestMethod("HEAD")
    huc.getResponseCode == HttpURLConnection.HTTP_OK
  }

}
