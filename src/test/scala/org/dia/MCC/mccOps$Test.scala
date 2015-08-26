package org.dia.MCC

import breeze.linalg.DenseMatrix
import org.dia.algorithms.mcc.mccOps
import org.dia.tensors.{AbstractTensor, BreezeTensor, Nd4jTensor}
import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite


class mccOps$Test extends FunSuite {

  test("testFindConnectedComponents") {
    val m = Array(
      Array(1.0, 2.0, 0.0, 4.0),
      Array(5.0, 6.0, 0.0, 8.0),
      Array(43.9, 23.0, 1.0,0.0),
      Array(0.0,0.0,0.0,0.0),
      Array(1.0, 0.0, 1.0, 0.0)
    )

    val cc = Array(
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 1.0, 0.0),
      Array(0.0, 0.0, 0.0, 0.0),
      Array(3.0, 0.0, 4.0, 0.0)
    )

    val ndArray = Nd4j.create(m)
    val ccArray = Nd4j.create(cc)
    val t = new Nd4jTensor(ndArray)
    val cct = new Nd4jTensor(ccArray)
    val labelled = mccOps.labelConnectedComponents(t)
    println(labelled)
    assert(labelled._1.equals(cct))
  }

  test("reduceResRectangle") {

    val m = Array(
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 1.0, 0.0),
      Array(0.0, 0.0, 0.0, 0.0),
      Array(3.0, 0.0, 4.0, 0.0)
    )

    val k = m.flatMap(p => p)
    val ndArray = new DenseMatrix(5, 4, k, 0, 4, true)
    val t: AbstractTensor = new BreezeTensor(ndArray)
    println()
    println(mccOps.reduceResolution(t, 5, 1))
    val reduced = mccOps.reduceRectangleResolution(t, 3, 3, 99999999)
    println(reduced)
  }

  test("findComponents") {
    val m = Array(
      Array(1.0, 2.0, 0.0, 4.0),
      Array(5.0, 6.0, 0.0, 8.0),
      Array(43.9, 23.0, 1.0,0.0),
      Array(0.0,0.0,0.0,0.0),
      Array(1.0, 0.0, 1.0, 0.0)
    )

    val cc = Array(
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 1.0, 0.0),
      Array(0.0,0.0,0.0,0.0),
      Array(3.0, 0.0, 4.0, 0.0)
    )

    val ndArray = Nd4j.create(m)
    val ccArray = Nd4j.create(cc)
    val t = new Nd4jTensor(ndArray)
    val cct = new Nd4jTensor(ccArray)
    val frames = mccOps.findConnectedComponents(t)
    val ccframes = mccOps.findConnectedComponents(cct)
    assert(frames == ccframes)
  }

}
