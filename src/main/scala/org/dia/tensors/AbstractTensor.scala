package org.dia.tensors

import org.slf4j.Logger
import ucar.nc2.dataset.NetcdfDataset

import scala.collection.mutable

/**
 * Created by rahulsp on 7/15/15.
 */
 trait AbstractTensor {
 type T <: AbstractTensor
  val name : String

  val LOG : Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  /**
   * Reduces the resolution of a DenseMatrix
   * @param blockSize the size of n x n size of blocks.
   * @return
   */
  def reduceResolution (blockSize: Int): T

  implicit def + (array : T) : T
}
