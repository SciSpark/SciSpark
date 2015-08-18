package org.dia.core


import java.io.Serializable

import org.dia.algorithms.mcc.mccOps
import org.dia.tensors.AbstractTensor

import scala.collection.mutable

class sciTensor(val variables: mutable.HashMap[String, AbstractTensor]) extends Serializable {

  val metaData: mutable.HashMap[String, String] = new mutable.HashMap[String, String]
  var varInUse = variables.toArray.apply(0)._1


  def this(variableName: String, array: AbstractTensor) {
    this(new mutable.HashMap[String, AbstractTensor] += ((variableName, array)))
  }

  def this(variableName: String, array: AbstractTensor, metaDataVar: (String, String)*) {
    this(variableName, array)
    metaDataVar.map(p => metaData += p)
  }

  def insertDictionary(metaDataVar: (String, String)*): Unit = {
    for (variable <- metaDataVar) metaData += variable
  }

  def apply(ranges: (Int, Int)*): sciTensor = {
    variables(varInUse)(ranges: _*)
  }

  def apply(variable: String): sciTensor = {
    varInUse = variable
    this
  }

  def <=(num: Double): sciTensor = variables(varInUse) <= num

  def reduceResolution(blockInt: Int): sciTensor = mccOps.reduceResolution(variables(varInUse), blockInt)

  def reduceRectangleResolution(rowblockSize: Int, colblockSize: Int): sciTensor = mccOps.reduceRectangleResolution(variables(varInUse), rowblockSize, colblockSize)

  def tensor : AbstractTensor = variables(varInUse)

  /**
   * TODO :: Use Stringbuilder
   * @return
   */
  override def toString: String = {
    var string = "Variable in use = " + varInUse + "\n" + variables.keys.toString + "\n"
    metaData.map(string += _ + "\n")
    string
  }

  private implicit def convert(tensor: AbstractTensor): sciTensor = new sciTensor(varInUse, tensor, metaData)

  def this(variableName: String, array: AbstractTensor, metaDataVar: mutable.HashMap[String, String]) {
    this(variableName, array)
    metaDataVar.map(p => metaData += p)
  }
}

