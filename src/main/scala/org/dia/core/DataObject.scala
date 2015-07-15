package org.dia.core

import org.nd4j.api.linalg.DSL._;
import org.nd4j.linalg.api.ndarray.INDArray

/**
 * Created by marroqui on 7/15/15.
 */
class DataObject (val metaData: Map[String,String], val iNDArray: INDArray) {

  def load(): Unit = {
    Nd4jFuncs.getNetCDFNDVars(, varName)
  }
  def joinMetadata(metaData: Map[String, String], metaData1: Map[String, String]): Map[String, String] = {
    null
  }

  def +(other:DataObject) : DataObject = {
    new DataObject(joinMetadata(other.metaData, this.metaData), (other.iNDArray + iNDArray))
  }

}
