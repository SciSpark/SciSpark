package org.dia.tensors

import org.dia.core.sliceableArray
import org.slf4j.Logger

/**
  */
trait AbstractTensor extends Serializable with sliceableArray {
  type T <: AbstractTensor
  type NumType = Double
  val name: String
  val LOG: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Reduces the resolution of a DenseMatrix
   * @param blockSize the size of n x n size of blocks.
   * @return
   */
  def reduceResolution(blockSize: Int): T



  /**
   * Elementwise Operations
   */

  def +(array: T): T

  def -(array: T): T

  def *(array: T): T

  def /(array: T): T

  def \(array: T): T

  /**
   * Linear Algebra Operations
   */

  def **(array: T): T


  /**
   * Masking operations
   */

  def <=(num: NumType): T


  /**
   * Utility Methods
   */

  def toString: String

  def equals(array: T): Boolean

  def shape: Array[Int]
}
