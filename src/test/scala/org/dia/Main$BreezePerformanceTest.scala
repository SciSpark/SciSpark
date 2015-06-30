package org.dia


import breeze.linalg.DenseMatrix
import org.scalatest.FunSuite
/**
 * This is a scala breeze implementation of the
 * metrics test in ocw. The purpose is to
 * test the performance of simple biasing functions
 *
 * Source : https://github.com/apache/climate/blob/master/ocw/metrics.py
 * Created by rahulsp on 6/22/15.
 */
class Main$BreezePerformanceTest extends FunSuite {

  // File URL leader
  val FILE_LEADER = "http://zipper.jpl.nasa.gov/dist/"
  // Two Local Model Files
  val FILE_1 = "AFRICA_KNMI-RACMO2.2b_CTL_ERAINT_MM_50km_1989-2008_tasmax.nc"
  val FILE_2 = "AFRICA_UC-WRF311_CTL_ERAINT_MM_50km-rg_1989-2008_tasmax.nc"
  test("ocwMetricsBreezeTest") {
    val knmi_dataset = Main.getBreezeNetCDFNDVars(FILE_LEADER + FILE_1, "tasmax")
    val wrf_dataset = Main.getBreezeNetCDFNDVars(FILE_LEADER + FILE_2, "tasmax")
    val result = new Array[DenseMatrix[Double]](knmi_dataset.length)
    val totalBefore = System.nanoTime()
    for(i <- 0 to knmi_dataset.length - 1){
//      val before = System.nanoTime()
      result(i) = knmi_dataset(i) - wrf_dataset(i)
//      val after = System.nanoTime()
//      println(after - before)
    }

    val totalAfter = System.nanoTime()
    println(totalAfter - totalBefore)
    assert(true)
  }

}
