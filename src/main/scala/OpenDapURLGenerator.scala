package org.dia

import java.io.PrintWriter
import java.io.File
import org.joda.time.DateTime

/**
 * Created by rahulsp on 6/19/15.
 */
object OpenDapURLGenerator {
  val url = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_L3/TRMM_3B42_daily/"
  val FILENAME = "TRMM_L3_Links.txt"



  def run() : Unit = {
    var dt = new DateTime(2000, 1, 2, 0, 0)
    val pw = new PrintWriter(new File(FILENAME))
//push
    for(j <- 0 to 1) {
      val limit = if(j % 4 == 0) 366 else 365
      for (i <- 1 to limit) {
        val paddedDay = (i.toString.reverse + "00").substring(0, 3).reverse
        val paddedMonth = (dt.getMonthOfYear.toString.reverse + "0").substring(0, 2).reverse
        val paddedMonthDay = (dt.getDayOfMonth.toString.reverse + "0").substring(0, 2).reverse
        pw.write(url + dt.getYear + "/" + paddedDay + "/" + "3B42_daily." + dt.getYear + "." + paddedMonth + "." + paddedMonthDay + ".7.bin\n")
        dt = dt.plusDays(1)
      }
    }
    pw.close()
  }
}
