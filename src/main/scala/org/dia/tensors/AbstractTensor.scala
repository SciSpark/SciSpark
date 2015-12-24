package org.dia.tensors

import org.slf4j.Logger

/**
 * An abstract tensor
 */
trait AbstractTensor extends Serializable with SliceableArray {

  type T <: AbstractTensor
  val name: String
  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def zeros(shape: Int*): T

  def map(f: Double => Double): AbstractTensor

  /**
   * Indexed Operations
   */

  def put(value: Double, shape: Int*): Unit

  /**
   * Element-wise Operations
   */

  def +(array: AbstractTensor): T

  def -(array: AbstractTensor): T

  def *(array: AbstractTensor): T

  def /(array: AbstractTensor): T

  def \(array: AbstractTensor): T

  /**
   * Linear Algebra Operations
   */

  def **(array: AbstractTensor): T

  /**
   * Masking operations
   */

  def <=(num: Double): T
  def :=(num: Double): T

  /**
   * Utility Methods
   */

  def cumsum: Double
  def toString: String

  override def equals(any: Any): Boolean = {
    val array = any.asInstanceOf[AbstractTensor]
    if (array.rows != this.rows) return false
    if (array.cols != this.cols) return false
    for (row <- 0 to array.rows - 1) {
      for (col <- 0 to array.cols - 1) {
        if (array(row, col) != this(row, col)) return false
      }
    }
    true
  }

  def shape: Array[Int]

  def isZero: Boolean
  def max: Double
  def min: Double

}
