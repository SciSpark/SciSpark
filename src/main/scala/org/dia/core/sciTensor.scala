package org.dia.core


import org.dia.tensors.AbstractTensor
import scala.collection.mutable


class sciTensor(val tensor : AbstractTensor) extends Serializable {

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

  implicit def +(array: sciTensor): sciTensor = tensor + array.tensor

  implicit def -(array: sciTensor): sciTensor = tensor - array.tensor

  implicit def \(array: sciTensor): sciTensor = tensor \ array.tensor

  implicit def /(array: sciTensor): sciTensor = tensor / array.tensor

  implicit def *(array: sciTensor): sciTensor = tensor * array.tensor

  /**
   * Linear Algebra Operations
   */
  implicit def **(array: sciTensor): sciTensor = tensor * array.tensor

  /**
   * Application Specific Operations
   */
  implicit def reduceResolution(blockSize : Int) : sciTensor = tensor.reduceResolution(blockSize)

  override def toString : String = tensor.toString

  def equals(array : sciTensor) : Boolean = tensor == array.tensor

  def data : Array[Double] = tensor.data
}
