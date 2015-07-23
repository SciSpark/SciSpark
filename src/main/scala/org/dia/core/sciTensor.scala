package org.dia.core


import org.dia.tensors.AbstractTensor

import scala.collection.mutable
import scala.collection.mutable.HashMap


class sciTensor(var variables : HashMap[String, AbstractTensor]) extends Serializable {

  var metaData : mutable.HashMap[String, String] = (new mutable.HashMap[String, String])
  var varInUse : String = ""

  def this(variableName : String, array : AbstractTensor){
    this(new mutable.HashMap[String, AbstractTensor])
    variables += ((variableName, array))
    varInUse = variableName
  }

  def this (variableName : String, array : AbstractTensor, metaDataVar : (String, String)*){
    this(variableName, array)
    metaDataVar.map(p => metaData += p)
  }

  def apply(variable : String) : sciTensor = {
    varInUse = variable
    this
  }

  implicit def convert(tensor : AbstractTensor) = new sciTensor(varInUse, tensor)

  def reduceResolution(blockInt : Int) :sciTensor = variables(varInUse).reduceResolution(blockInt)
}

