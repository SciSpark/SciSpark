package org.dia.tensors

import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite
import org.nd4s.Implicits._

/**
 * Tests basic tensor functionality.
 */
class BasicTensorTest extends FunSuite {

  test("filter") {
    val dense = Nd4j.create(Array[Double](1, 241, 241, 1), Array(2, 2))
    val t = dense.map(p => if (p < 241.0) p else 0.0)
    println(t)
    assert(true)
  }

  test("Nd4sSlice") {
    val nd = Nd4j.create((0d to 8d by 1d).toArray, Array(4, 2))
    println(nd)
    println(nd(0 -> 1, ->))
    assert(false)
  }

}