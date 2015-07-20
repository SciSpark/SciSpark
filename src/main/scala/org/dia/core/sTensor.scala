package org.dia.core


import org.dia.tensors.AbstractTensor

import scala.collection.mutable


class sTensor(val tensor : AbstractTensor) {

  var metaData : mutable.HashMap[String, String] = (new mutable.HashMap[String, String])
  def this (tensor : AbstractTensor, metaDataVar : (String, String)*){
    this(tensor)
    metaDataVar.map(p => metaData += p)
  }

//  def +(other:sTensor) : sTensor = {
//    new sTensor(joinMetadata(other.metaData, this.metaData), other.iNDArray + iNDArray)
//  }

}
