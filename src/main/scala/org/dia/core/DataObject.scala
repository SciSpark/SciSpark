package org.dia.core


import org.apache.spark.mllib.linalg.DenseMatrix
import org.nd4j.linalg.api.ndarray.INDArray

import scala.collection.mutable


class DataObject (val arrayLibrary : ArrayLib)   {
  var metaData : mutable.HashMap[String, String] = new mutable.HashMap[String, String]


//  def +(other:DataObject) : DataObject = {
//    //new DataObject(joinMetadata(other.metaData, this.metaData), (other.iNDArray + iNDArray))
//  }

}
