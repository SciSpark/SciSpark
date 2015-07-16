package org.dia.core


import org.apache.spark.mllib.linalg.DenseMatrix
import org.nd4j.linalg.api.ndarray.INDArray

import scala.collection.mutable

/**
 * Created by marroqui on 7/15/15.
 */
class DataObject (var arrayLibrary : ArrayLib[INDArray, DenseMatrix[Double]])   {

  var metaData : mutable.HashMap[String, String] = new mutable.HashMap[String, String]

  def joinMetadata(metaData: Map[String, String], metaData1: Map[String, String]): Map[String, String] = {
    null
  }

//  def +(other:DataObject) : DataObject = {
//    //new DataObject(joinMetadata(other.metaData, this.metaData), (other.iNDArray + iNDArray))
//  }

}
