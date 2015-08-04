package org.dia.tensors

trait sliceableArray {
  type T <: sliceableArray

  def rows: Int

  def cols: Int

  def shape: Array[Int]

  def data: Array[Double]


  def apply(ranges: (Int, Int)*): T

  def apply(indexes: Int*): Double
}
