package org.dia.core


import breeze.linalg.DenseMatrix
import org.dia.tensors.AbstractTensor

import scala.collection.mutable


class sciTensor(val tensor : AbstractTensor) {

  var metaData : mutable.HashMap[String, String] = (new mutable.HashMap[String, String])
  def this (tensor : AbstractTensor, metaDataVar : (String, String)*){
    this(tensor)
    metaDataVar.map(p => metaData += p)
  }

  implicit def convert(array : AbstractTensor) = new sciTensor(array)
  implicit def typeConvert(array : AbstractTensor) : this.tensor.T = {
    if(array.getClass != this.tensor.getClass) {
     throw new Exception("Incompatible types" + this.tensor.getClass + " with " + array.getClass)
    }
    array.asInstanceOf[this.tensor.T]
  }

  /**
   * Due to implicit conversions we can do operations on sTensors and DenseMatrix
   */

  def +(array: sciTensor): sciTensor = tensor + array.tensor

//  override implicit def -(array: sTensor): sTensor = tensor - array.tensor
//
//  override implicit def \(array: sTensor): sTensor = tensor \ array.tensor
//
//  override implicit def /(array: sTensor): sTensor = tensor / array.tensor
//
//  override implicit def *(array: sTensor): sTensor = tensor :* array.tensor

  /**
   * Linear Algebra Operations
   */
//  override implicit def **(array: sTensor): sTensor = tensor * array.tensor

  override def toString : String = tensor.toString
//  def +(other:sTensor) : sTensor = {
//    new sTensor(joinMetadata(other.metaData, this.metaData), other.iNDArray + iNDArray)
//  }

}
