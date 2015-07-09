package org.dia.core

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

/**
 * Created by rahulsp on 7/9/15.
 */
class SciSparkContextTest extends FunSuite {

  test("testOpenDapURLFile") {

    val scisparkContext = new SciSparkContext("local[4]", "test")
    val srdd = scisparkContext.OpenDapURLFile("TestLinks", "TotCldLiqH2O_A")

    val collected = srdd.collect
    collected.map(p => println(p))
    assert(true)
  }

}
