package org.dia.mcc

import breeze.linalg.DenseMatrix
import org.dia.algorithms.mcc.MCCOps
import org.dia.tensors.{ AbstractTensor, BreezeTensor, Nd4jTensor }
import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite

/**
 * Tests functionality in MCCOpsTest including:
 * reduceResolution,labelConnectedComponents,findConnectedComponents.
 */
class MCCOpsTest extends FunSuite {

  /**
   * Note that Nd4s slicing is broken at the moment
   */
  test("reduceResolutionTest") {
    val dense = new DenseMatrix[Double](4, 2, (0d to 8d by 1d).toArray, 0, 2, true)
    val nd = Nd4j.create((0d to 8d by 1d).toArray, Array(4, 2))
    val breeze = new BreezeTensor((0d to 8d by 1d).toArray, Array(4, 2))
    val nd4j = new Nd4jTensor(nd)
    println("breeze")
    val breezeReduced = MCCOps.reduceResolution(breeze, 2, 999999)
    println("nd4j")
    val nd4jReduced = MCCOps.reduceResolution(nd4j, 2, 999999)

    println(breeze)
    println(nd4j)

    if (breeze == nd4j) println("THESE ARE TRUE TRUE TRUE")

    println(breezeReduced)
    println(nd4jReduced)

    println(breezeReduced.data.toList)
    println(nd4jReduced.data.toList)

    assert(breezeReduced == nd4jReduced)
  }

  test("testFindConnectedComponents") {
    val m = Array(
      Array(1.0, 2.0, 0.0, 4.0),
      Array(5.0, 6.0, 0.0, 8.0),
      Array(43.9, 23.0, 1.0, 0.0),
      Array(0.0, 0.0, 0.0, 0.0),
      Array(1.0, 0.0, 1.0, 0.0))

    val cc = Array(
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 1.0, 0.0),
      Array(0.0, 0.0, 0.0, 0.0),
      Array(3.0, 0.0, 4.0, 0.0))

    val ndArray = Nd4j.create(m)
    val ccArray = Nd4j.create(cc)
    val t = new Nd4jTensor(ndArray)
    val cct = new Nd4jTensor(ccArray)
    val labelled = MCCOps.labelConnectedComponents(t)
    println(labelled)
    assert(labelled._1.equals(cct))
  }

  test("reduceResRectangle") {

    val m = Array(
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 1.0, 0.0),
      Array(0.0, 0.0, 0.0, 0.0),
      Array(3.0, 0.0, 4.0, 0.0))

    val k = m.flatMap(p => p)
    val ndArray = new DenseMatrix(5, 4, k, 0, 4, true)
    val t: AbstractTensor = new BreezeTensor(ndArray)
    println()
    println(MCCOps.reduceResolution(t, 5, 1))
    val reduced = MCCOps.reduceRectangleResolution(t, 3, 3, 99999999)
    println(reduced)
  }

  test("findComponents") {
    val m = Array(
      Array(1.0, 2.0, 0.0, 4.0),
      Array(5.0, 6.0, 0.0, 8.0),
      Array(43.9, 23.0, 1.0, 0.0),
      Array(0.0, 0.0, 0.0, 0.0),
      Array(1.0, 0.0, 1.0, 0.0))

    val cc = Array(
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 1.0, 0.0),
      Array(0.0, 0.0, 0.0, 0.0),
      Array(3.0, 0.0, 4.0, 0.0))

    val ndArray = Nd4j.create(m)
    val ccArray = Nd4j.create(cc)
    val t = new Nd4jTensor(ndArray)
    val cct = new Nd4jTensor(ccArray)
    val frames = MCCOps.findConnectedComponents(t)
    val ccframes = MCCOps.findConnectedComponents(cct)
    assert(frames == ccframes)
  }

}
