package org.dia.core


import org.dia.tensors.AbstractTensor

import scala.collection.mutable
import scala.collection.mutable.HashMap


class sciTensor(val variables : HashMap[String, AbstractTensor]) extends Serializable {

  val metaData : mutable.HashMap[String, String] = (new mutable.HashMap[String, String])
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

  implicit def convert(tensor : AbstractTensor) = new sciTensor(varInUse, tensor)

  def apply(ranges : (Int, Int)*) : sciTensor = {
    variables(varInUse)(ranges:_*)
  }

  def apply(variable : String) : sciTensor = {
    varInUse = variable
    this
  }

  def <=(num : Double) : sciTensor = variables(varInUse) <= num
  def reduceResolution(blockInt : Int) :sciTensor = variables(varInUse).reduceResolution(blockInt)

  override def toString : String = variables(varInUse).toString
}

