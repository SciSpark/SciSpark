package org.dia.tensors

import org.slf4j.Logger

/**
 * An abstract tensor
 */
trait AbstractTensor extends Serializable with SliceableArray {

  type T <: AbstractTensor
  val name: String
  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def reshape(shape: Array[Int]): T
  def broadcast(shape: Array[Int]): T
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
  def +(scalar: Double): T

  def -(array: AbstractTensor): T 
  def -(scalar: Double): T

  def *(array: AbstractTensor): T
  def *(scalar: Double): T

  def /(array: AbstractTensor): T
  def /(scalar: Double): T

  def :+(array: AbstractTensor): T
  def :+(scalar: Double): T

  def :-(array: AbstractTensor): T 
  def :-(scalar: Double): T

  def :*(array: AbstractTensor): T
  def :*(scalar: Double): T

  def :/(array: AbstractTensor): T
  def :/(scalar: Double): T

  /**
   * Linear Algebra Operations
   */

  def **(array: AbstractTensor): T
  
  def div(num: Double): T

  /**
   * Masking operations
   */
  def mask(f: Double => Boolean, mask: Double) : T
  def setMask(num: Double): T
  def <(num: Double): T
  def >(num: Double): T
  def <=(num: Double): T
  def >=(num: Double): T
  def :=(num: Double): T
  def !=(num: Double): T
  
  /**
   * Returns the data as a flattened array
   *
   */
  def data: Array[Double]

   /**
   * Returns the data dimensions
   *
   */
  def shape: Array[Int]
  
  /**
   * Utility Methods
   */

  def cumsum: Double
  def mean(axis : Int*) : T
  def detrend(axis: Int) : T
  def toString: String

  override def equals(any: Any): Boolean = {
    val array = any.asInstanceOf[AbstractTensor]
    val shape = array.shape
    val thisShape = this.shape

    if(!shape.sameElements(thisShape)) return false

    val thisData = this.data
    val otherData = array.data
    for(index <- 0 to thisData.length - 1) if(thisData(index) != otherData(index)) return false
    true
  }

  

  def isZero: Boolean
  /**
   *  Shortcut test whether tensor is zero
   *  in case we know its entries are all non-negative.
   */
  def isZeroShortcut: Boolean
  def max: Double
  def min: Double

}
