package org.dia.core


import java.io.Serializable
import org.dia.algorithms.mcc.mccOps
import org.dia.tensors.AbstractTensor
import scala.collection.mutable
import org.dia.sLib

class sciTensor(val variables: mutable.HashMap[String, AbstractTensor]) extends Serializable {

  val metaData: mutable.HashMap[String, String] = new mutable.HashMap[String, String]
  val head = variables.toArray
  var varInUse = variables.toArray.apply(0)._1


  def this(variableName: String, array: AbstractTensor) {
    this(new mutable.HashMap[String, AbstractTensor] += ((variableName, array)))
  }

  def this(variableName: String, array: AbstractTensor, metaDataVar: (String, String)*) {
    this(variableName, array)
    metaDataVar.map(p => metaData += p)
  }

  def this(variableName: String, array: AbstractTensor, metaDataVar : mutable.HashMap[String, String]) {
    this(variableName, array)
    metaDataVar.map(p => metaData += p)
  }

  def insertDictionary(metaDataVar: (String, String)*): Unit = {
    for (variable <- metaDataVar) metaData += variable
  }

  private implicit def convert(tensor: AbstractTensor): sciTensor = new sciTensor(varInUse, tensor, metaData)


  def apply(ranges: (Int, Int)*): sciTensor = {
    variables(varInUse)(ranges: _*)
  }

  def apply(variable: String): sciTensor = {
    varInUse = variable
    this
  }

  def <=(num: Double): sciTensor = variables(varInUse) <= num

  def reduceResolution(blockInt: Int): sciTensor = mccOps.reduceResolution(variables(varInUse), blockInt)

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
}

