package org.dia.core

import org.dia.TestEnvironment.SparkTestConstants
import org.dia.loaders.NetCDFReader._
import org.dia.partitioners.STrmmPartitioner._
import org.dia.urlgenerators.HourlyTrmmURLGenerator;
import org.scalatest.FunSuite

/**
 * Tests for proper construction of SRDD's from URIs.
 */
class SRDDTest extends FunSuite {
  
  val sc = SparkTestConstants.sc
  val testLinks = SparkTestConstants.datasetPath

  test("GroupingByYearPartitioning") {
    val urls = HourlyTrmmURLGenerator.generateTrmmDaily(1999, 2000).toList.slice(0, 2)
    sc.setLocalProperty("log4j.configuration", "resources/log4j-defaults.properties")
    val sRDD = new SRDD[SciTensor](sc.sparkContext, urls, List("precipitation"), loadNetCDFNDVar, mapOneYearToManyTensorTRMM)
    val Tensor = sRDD.collect()(0)
    assert(true)
  }

  test("GroupingByDayPartitioning") {
    val urls = HourlyTrmmURLGenerator.generateTrmmDaily(1999, 2000).toList.slice(0, 2)
    sc.setLocalProperty("log4j.configuration", "resources/log4j-defaults.properties")
    val sRDD = new SRDD[SciTensor](sc.sparkContext, urls, List("precipitation"), loadNetCDFNDVar, mapOneDayToManyTensorTRMM)
    val Tensor = sRDD.collect()(0)
    assert(true)
  }

}
