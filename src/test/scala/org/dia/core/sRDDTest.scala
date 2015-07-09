package org.dia.core

import org.apache.spark.SparkContext
import org.dia.NetCDFUtils
import org.nd4j.linalg.api.ndarray.INDArray
import org.scalatest.FunSuite

import scala.io.Source

/**
 * Created by rahulsp on 7/9/15.
 */
class sRDDTest extends FunSuite {
  test("basicFunctionality") {
    val sc = SparkTestConstants.sc
    val datasetUrls = Source.fromFile("TestLinks").mkString.split("\n").toList
    val srdd = new sRDD[INDArray](sc, datasetUrls, "TotCldLiqH2O_A")

    val collected = srdd.collect
    collected.map(p => println(p))
    sc.stop()
    assert(true)
  }
}
