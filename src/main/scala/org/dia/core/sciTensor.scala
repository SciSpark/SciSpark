package org.dia.core


import java.io.Serializable

import org.dia.algorithms.mcc.mccOps
import org.dia.tensors.AbstractTensor
import org.slf4j.Logger

import scala.collection.mutable

/**
 * The sciTensor is a self documented array object. It stores N number of variable arrays.
 * The sciTensor also keeps track of a metadata table for properties which the user may want to record.
 * Note that all linear algebra and ocw operations on sciTensors are performed on the variable in use.
 * Furthermore sciTensors are treated as immutable objects and so all operations return new sciTensor objects.
 *
 * @param variables a hasmap of variable arrays
 */
class sciTensor(val variables: mutable.HashMap[String, AbstractTensor]) extends Serializable {

  val LOG: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  val metaData: mutable.HashMap[String, String] = new mutable.HashMap[String, String]
  var varInUse = variables.toArray.apply(0)._1


  def this(variableName: String, array: AbstractTensor) {
    this(new mutable.HashMap[String, AbstractTensor] += ((variableName, array)))
  }

  def this(variableName: String, array: AbstractTensor, metaDataVar: (String, String)*) {
    this(variableName, array)
    metaDataVar.map(p => metaData += p)
  }

  /**
   * Writes metaData in the form of key-value pairs
   */
  def insertDictionary(metaDataVar: (String, String)*): Unit = {
    for (variable <- metaDataVar) metaData += variable
  }

  /**
   * Slices the head variable array given the list of ranges per dimension.
   */
  def apply(ranges: (Int, Int)*): sciTensor = {
    variables(varInUse)(ranges: _*)
  }

  /**
   * Shifts the variable in use pointer to a different variable array.
   * If the variable is not found, error message to log4j.
   */
  def apply(variable: String): sciTensor = {
    if (variables.keySet.contains(variable)) {
      varInUse = variable
    } else {
      LOG.error("Variable " + variable + " was NOT FOUND in the variable array table.")
    }
    this
  }

  /**
   * returns the variable array that is currently being used
   */
  def tensor: AbstractTensor = variables(varInUse)

  /**
   * Masks the current variable array by preserving values
   * less than or equal to num.
   */
  def <=(num: Double): sciTensor = variables(varInUse) <= num

  /**
   * Returns a block averaged matrix where the blocks are squares with
   * dimensions blockInt.
   */
  def reduceResolution(blockInt: Int, invalid: Int): sciTensor = {
    mccOps.reduceResolution(variables(varInUse), blockInt, invalid)
  }

  /**
   * ------------------------------ Matrix Operations ---------------------------------
   * The following functions are Matrix Operations specific to SciSpark and it's goals.
   */

  /**
   * Returns a block averaged matrix where the blocks are rectangles with dimensions
   * rowblockSize X colblockSize.
   */
  def reduceRectangleResolution(rowblockSize: Int, colblockSize: Int, invalid: Int): sciTensor = {
    mccOps.reduceRectangleResolution(variables(varInUse), rowblockSize, colblockSize, invalid)
  }

  override def toString: String = {
    var string = "Variable in use = " + varInUse + "\n" + variables.keys.toString + "\n"
    metaData.foreach(string += _ + "\n")
    string
  }

  /**
   * An implicit converter that is called on every sciTensor operator function.
   */
  private implicit def convert(tensor: AbstractTensor): sciTensor = new sciTensor(varInUse, tensor, metaData)

  def this(variableName: String, array: AbstractTensor, metaDataVar: mutable.HashMap[String, String]) {
    this(variableName, array)
    metaDataVar.map(p => metaData += p)
  }


}

