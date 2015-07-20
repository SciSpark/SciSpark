package org.dia.tensors

import breeze.linalg.DenseMatrix
import org.nd4j.linalg.api.ndarray.INDArray
import org.slf4j.Logger
import ucar.nc2.dataset.NetcdfDataset

import scala.collection.mutable

/**
 * Created by rahulsp on 7/15/15.
 */

 abstract class AbstractTensor {
 type T <: AbstractTensor
  val name : String
  val LOG : Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Reduces the resolution of a DenseMatrix
   * @param blockSize the size of n x n size of blocks.
   * @return
   */
  def reduceResolution (blockSize: Int): T

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

  implicit def apply : T


}
