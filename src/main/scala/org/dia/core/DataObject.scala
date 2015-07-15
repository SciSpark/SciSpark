package org.dia.core

import org.dia.core.ArrayLib
import org.dia.n.Nd4jLib
import org.nd4j.api.linalg.DSL._;
import org.nd4j.linalg.api.ndarray.INDArray
import breeze.linalg.DenseMatrix
/**
 * Created by marroqui on 7/15/15.
 */
class DataObject  {
//  val metaData : Map[String, String]
//  val array : ArrayLib


//  def this(url : String, varName : String, arrayLib : ArrayLib){
//    this
////  arrayLib match {
////    case ND4J
////  }
//  }

  def joinMetadata(metaData: Map[String, String], metaData1: Map[String, String]): Map[String, String] = {
    null
  }

//  def +(other:DataObject) : DataObject = {
//    //new DataObject(joinMetadata(other.metaData, this.metaData), (other.iNDArray + iNDArray))
//  }

}
