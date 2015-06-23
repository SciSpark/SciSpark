package org.dia

import java.io.{File, PrintWriter}
import java.net.{HttpURLConnection, URL}
import java.util

import org.joda.time.DateTime

import scala.collection.JavaConversions._

/**
 * Created by rahulsp on 6/19/15.
 */
object OpenDapURLGenerator {
  val URL = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_L3/TRMM_3B42_daily/"
  val FILENAME = "TRMM_L3_Links2.txt"
  val iniYear = 1997
  val endYear = 2015

  def run() : Unit = {

    val numYears = (0 to (endYear - iniYear)).toList
    // reading time
    var readTime = new DateTime(1997, 1, 2, 0, 0)
    val pw = new PrintWriter(new File(FILENAME))
    pw.flush()
    val totalUrls = new util.ArrayList[String]()
    try {
      numYears.par.foreach { e => totalUrls.addAll(generateLinksPerYear(e)) }
      println("Total URLs: " + totalUrls.size())
      totalUrls.foreach { e => pw.append(e.toString + "\n") }
    } catch {
      case ex: Exception => {
        println("Exception")
        ex.printStackTrace()
      }
    } finally {
      pw.close()
    }
  }
    /**
     * Gets the links per year
     * @param year
     * @return
     */
    def generateLinksPerYear(year: Int) : util.ArrayList[String] = {

      val checkedYear = iniYear + year
      val urls = new util.ArrayList[String]()
      val days = if(checkedYear % 4 == 0) 366 else 365
      var readTime = new DateTime(checkedYear, 1, 2, 0, 0)
      //for each year try to generate each day
      for (day <- 1 to days) {
        val paddedDay = (day.toString.reverse + "00").substring(0, 3).reverse
        val paddedMonth = (readTime.getMonthOfYear.toString.reverse + "0").substring(0, 2).reverse
        val paddedReadDay = (readTime.getDayOfMonth.toString.reverse + "0").substring(0, 2).reverse
        readTime = readTime.plusDays(1)

        val sb = new StringBuilder()
        sb.append(checkedYear).append("/");
        sb.append(paddedDay).append("/");
        sb.append("3B42_daily.").append(readTime.getYear).append(".");
        sb.append(paddedMonth).append(".");
        sb.append(paddedReadDay).append(".7.bin");
        // check url and stop if it doesn't exist
        val tmpUrl = URL + sb.toString
        if (getResponseCode(tmpUrl)) {
          urls.add(tmpUrl)
          //println(tmpUrl)
        }
      }
      return urls;
    }

    /**
     * Checks if the url actually exists
     * @param urlString
     * @return
     */
    def getResponseCode(urlString : String):Boolean = {
      val u = new URL(urlString);
      val huc =  u.openConnection().asInstanceOf[HttpURLConnection];
      huc.setConnectTimeout(100000);
      huc.setRequestMethod("HEAD");
      return (huc.getResponseCode() == HttpURLConnection.HTTP_OK);
    }

}
