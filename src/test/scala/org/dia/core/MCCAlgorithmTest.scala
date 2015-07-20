package org.dia.core

import org.scalatest.FunSuite
import org.dia.Constants._
/**
 * Created by rahulsp on 7/20/15.
 */
class MCCAlgorithmTest extends FunSuite {
  test("mappedReduceResolutionTest") {
    val sc = SparkTestConstants.sc
    val nd4jRDD = sc.OpenDapURLFile("TestLinks", "TotCldLiqH2O_A")
    val collect = nd4jRDD.collect
    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)

    val col = sc.OpenDapURLFile("TestLinks", "TotCldLiqH2O_A").collect
    assert(collect(0).tensor.getUnderlying() == col(0).tensor.getUnderlying())
  }
}
