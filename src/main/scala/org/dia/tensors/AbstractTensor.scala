package org.dia.tensors

import org.slf4j.Logger



/**
  */
trait AbstractTensor extends Serializable with sliceableArray {
  type T <: AbstractTensor
  val name: String
  val LOG: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Reduces the resolution of a DenseMatrix
   * @param blockSize the size of n x n size of blocks.
   * @return
   */
  def reduceResolution(blockSize: Int): T

  def zeros(shape: Int*): T

  def map(f : Double => Double) : AbstractTensor
  /**
   * Indexed Operations
   */

  def put(value: Double, shape: Int*): Unit

  /**
   * Elementwise Operations
   */

  def +(array: T): T

  def -(array: T): T

  def *(array: AbstractTensor): T

  def /(array: T): T

  def \(array: T): T

  /**
   * Linear Algebra Operations
   */

  def **(array: T): T


  /**
   * Masking operations
   */

  def <=(num: Double): T
  def :=(num : Double): T

  /**
   * Utility Methods
   */

  def cumsum: Double
  def toString: String

  def equals(array: T): Boolean

  def shape: Array[Int]

  def max: Double
  def min: Double
}
