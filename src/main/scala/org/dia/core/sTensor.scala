package org.dia.core


import org.apache.spark.mllib.linalg.DenseMatrix
import org.dia.tensors.AbstractTensor
import org.nd4j.linalg.api.ndarray.INDArray

import scala.collection.mutable


class sTensor() {
  var metaData : mutable.HashMap[String, String] = new mutable.HashMap[String, String]
  val arrayLibrary : AbstractTensor = _

//  def +(other:sTensor) : sTensor = {
//    new sTensor(joinMetadata(other.metaData, this.metaData), other.iNDArray + iNDArray)
//  }

}
