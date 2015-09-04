package org.dia.core

import org.dia.TestEnvironment.SparkTestConstants
import org.dia.URLGenerator.HourlyTrmmUrlGenerator
import org.dia.loaders.NetCDFReader._
import org.dia.partitioners.sTrmmPartitioner._
import org.scalatest.FunSuite

/**
 * Tests for proper construction of sRDD's.
 */
class sRDDTest extends FunSuite {
  val sc = SparkTestConstants.sc
  val TestLinks = SparkTestConstants.datasetPath

  test("GroupingByYearPartitioning") {
    val urls = HourlyTrmmUrlGenerator.generateTrmmDaily(1999, 2000).toList.slice(0, 2)
    sc.setLocalProperty("log4j.configuration", "resources/log4j-defaults.properties")

    val sRdd = new sRDD[sciTensor](sc.sparkContext, urls, List("precipitation"), loadNetCDFNDVars, mapOneYearToManyTensorTRMM)
    val Tensor = sRdd.collect()(0)
    assert(true)
  }

  test("GroupingByDayPartitioning") {
    val urls = HourlyTrmmUrlGenerator.generateTrmmDaily(1999, 2000).toList.slice(0, 2)
    sc.setLocalProperty("log4j.configuration", "resources/log4j-defaults.properties")

    val sRdd = new sRDD[sciTensor](sc.sparkContext, urls, List("precipitation"), loadNetCDFNDVars, mapOneDayToManyTensorTRMM)
    val Tensor = sRdd.collect()(0)
    assert(true)
  }
}
