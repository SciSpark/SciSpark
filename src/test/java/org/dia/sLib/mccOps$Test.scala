package org.dia.sLib

import org.dia.tensors.Nd4jTensor
import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite
import org.nd4j.api.Implicits._

/**
 * Created by rahulsp on 8/4/15.
 */
class mccOps$Test extends FunSuite {

  test("testFindConnectedComponents") {
    val m = Array(
      Array(1.0, 2.0, 0.0, 4.0),
      Array(5.0, 6.0, 0.0, 8.0),
      Array(43.9, 23.0, 1.0,0.0),
      Array(0.0,0.0,0.0,0.0),
      Array(1.0, 0.0, 1.0, 0.0)
    )

    val ndArray = Nd4j.create(m)
    val t = new Nd4jTensor(ndArray)
    val labelled = mccOps.labelConnectedComponents(t)
    //println(labelled)
    assert(true)
  }

  test("findComponents") {
    val m = Array(
      Array(1.0, 2.0, 0.0, 4.0),
      Array(5.0, 6.0, 0.0, 8.0),
      Array(43.9, 23.0, 1.0,0.0),
      Array(0.0,0.0,0.0,0.0),
      Array(1.0, 0.0, 1.0, 0.0)
    )

    val ndArray = Nd4j.create(m)
    val t = new Nd4jTensor(ndArray)
    val frames = mccOps.findConnectedComponents(t)
    frames.map(p => {
      println(p)
      println(mccOps.areaFilled(p))
    })

    assert(true)
  }

  test("testBACKGROUND") {
    val m = Array(
      Array(1.0, 2.0, 0.0, 4.0),
      Array(5.0, 6.0, 0.0, 8.0),
      Array(43.9, 23.0, 1.0,0.0),
      Array(0.0,0.0,0.0,0.0),
      Array(1.0, 0.0, 1.0, 0.0)
    )

    val ndArray = Nd4j.create(m)
    println(ndArray)
    val t = ndArray.map(p => if(p <= 23.0) p else 0.0)
    println(t)
    assert(true)
  }

  test("testReduceResolution") {

  }

}
