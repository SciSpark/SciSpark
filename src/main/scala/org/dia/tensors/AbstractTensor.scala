package org.dia.tensors

import org.slf4j.Logger

/**
 * Created by rahulsp on 7/15/15.
 */
 trait AbstractTensor  extends Serializable {

 type T <: AbstractTensor
  val name : String
  val LOG : Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Reduces the resolution of a DenseMatrix
   * @param blockSize the size of n x n size of blocks.
   * @return
   */
  def reduceResolution (blockSize: Int): T

 implicit def data : Array[Double]
  /**
   * Elementwise Operations
   */

  implicit def + (array : T) : T
  implicit def - (array : T) : T

  implicit def *(array : T) : T

  implicit def /(array : T) : T

  implicit def \ (array : T) : T

  /**
   * Linear Algebra Operations
   */

  implicit def **(array : T) : T


  /**
   * In place operations
   */

//  implicit def +=(array : T) : T
//
//  implicit def -=(array : T) : T
//
//  implicit def *=(array : T) : T
//
//  implicit def **=(array : T) : T
//
//  implicit def /=(array : T) : T
//
//  implicit def \=(array : T) : T

  def toString : String

  def equals(array : T) : Boolean

  def getUnderlying() : (Array[Double], Array[Int]) = (data, shape)

  def data : Array[Double]

  def shape : Array[Int]
}
