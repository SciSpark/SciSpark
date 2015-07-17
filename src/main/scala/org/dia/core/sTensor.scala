package org.dia.core


import org.dia.tensors.AbstractTensor

import scala.collection.mutable


class sTensor(val urlValue : String) {
  var metaData : mutable.HashMap[String, String] = (new mutable.HashMap[String, String]) += (("URI", urlValue))
  var tensor : AbstractTensor = null

//  def +(other:sTensor) : sTensor = {
//    new sTensor(joinMetadata(other.metaData, this.metaData), other.iNDArray + iNDArray)
//  }

}
