package org.dia.core

import org.dia.tensors.AbstractTensor

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
 * Created by rahulsp on 7/23/15.
 */
class NDArray(var variables : HashMap[String, sciTensor]) {

  var metaData : mutable.HashMap[String, String] = (new mutable.HashMap[String, String])

  def this(variableName : String, array : sciTensor){
    this(new mutable.HashMap[String, sciTensor]())
    variables += ((variableName, array))
  }

  def this (variableName : String, array : sciTensor, metaDataVar : (String, String)*){
    this(variableName, array)
    metaDataVar.map(p => metaData += p)
  }
  def apply(variable : String) : sciTensor = variables(variable)
}
