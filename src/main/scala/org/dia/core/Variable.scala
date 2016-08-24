/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dia.core

import java.io.Serializable

import scala.collection.{mutable, TraversableOnce}
import scala.collection.JavaConverters._

import ucar.nc2.Attribute

import org.dia.algorithms.mcc.MCCOps
import org.dia.tensors.{AbstractTensor, Nd4jTensor}
import org.dia.utils.NetCDFUtils

/**
 * A Variable is a logical container for data.
 * It has a dataType, a shape, and a set of Attributes.
 *
 * The data is a multidimensional array.
 * Data access is done through the apply() method,
 * which returns the multidimensional array as an AbstractTensor.
 *
 * An important feature of Variable objects is that an empty apply function
 * returns the underlying AbstractTensor i.e. the core array.
 * This is to mimic the following python syntax :
 *
 *            numpyArray = netcdfVar[:]
 *
 * In SciSpark it is done like this:
 *
 *            AbstractTensor = netcdfVar()
 */
class Variable(var name: String,
               val dataType: String,
               val array: AbstractTensor,
               val attributes: mutable.HashMap[String, String],
               val dims: List[(String, Int)]) extends Serializable {

  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def this(name: String,
           dataType: String,
           array: Array[Double],
           shape: Array[Int],
           attr: Seq[(String, String)],
           dims: List[(String, Int)]) {
    this(name,
         dataType,
         new Nd4jTensor(array, shape),
         mutable.HashMap[String, String]() ++= attr,
         dims)
  }


  def this(name: String, nvar: ucar.nc2.Variable) {
    this(name,
         nvar.getDataType.toString,
         NetCDFUtils.getArrayFromVariable(nvar),
         nvar.getShape,
         nvar.getAttributes.asScala.map(p => NetCDFUtils.convertAttribute(p)),
         nvar.getDimensions.asScala.map(p => (p.getFullName, p.getLength)).toList)
  }

  def this(nvar: ucar.nc2.Variable) {
    this(nvar.getFullName, nvar)
  }

  def this(name: String, array: AbstractTensor) {
    this(name,
         "Double64",
         array,
         mutable.HashMap[String, String](),
         List[(String, Int)]())
  }

  def this(array: AbstractTensor) {
    this("unnamed", array)
  }

  /**
   * Writes attribute in the form of key-value pairs
   */
  def insertAttributes(metaDataVar: (String, String)*): Unit = {
    for (variable <- metaDataVar) attributes += variable
  }

  def insertAttributes(metaDataVars: TraversableOnce[(String, String)]): Unit = {
    attributes ++= metaDataVars
  }

  /**
   * Returns the array corresponding to the variable in use.
   * This is to mimic the numpy like syntax of var[:]
   * Example usage: val absT = var()
   *
   * @return AbstractTensor corresponding to variable in use
   */
  def apply(): AbstractTensor = array

  /**
   * Access attribute values.
   * In Python's netcdf variable, attributes can be accessed as
   * members of classes like so:
   *    variable.attribute1
   * In scala we can't do that so we access attributes with the
   * apply function like so:
   * variable("attribute")
   *
   * @param key the attribute name
   * @return the attribute value
   */
  def apply(key: String): String = attributes(key)

  def apply(index: Int): Variable = arrayOp(this()(index), index.toString)

  def apply(index: (Int, Int)*): Variable = {
    arrayOp(this()(index: _*), index.map( {case (a, b) => a + ":" + b}).reduce((a, b) => a + "," + b))
  }

  def shape(): Array[Int] = array.shape

  def data(): Array[Double] = array.data

  /**
   * Linear Algebra Operations
   */
  def **(other: Variable): Variable = varOp(this() ** other(), "**", other.name)

  def +(other: Variable): Variable = varOp(this() + other(), "+", other.name)

  def +(scalar: Double): Variable = varOp(this() + scalar, "+", scalar.toString)

  def -(other: Variable): Variable = varOp(this() - other(), "-", other.name)

  def -(scalar: Double): Variable = varOp(this() - scalar, "-", scalar.toString)

  def /(other: Variable): Variable = varOp(this() / other(), "/", other.name)

  def /(scalar: Double): Variable = varOp(this() / scalar, "/", scalar.toString)

  def *(other: Variable): Variable = varOp(this() * other(), "*", other.name)

  def *(scalar: Double): Variable = varOp(this() * scalar, "*", scalar.toString)

  def +=(other: Variable): Variable = varOp(this() += other(), "+=", other.name)

  def +=(scalar: Double): Variable = varOp(this() += scalar, "+=", scalar.toString)

  def -=(other: Variable): Variable = varOp(this() -= other(), "-=", other.name)

  def -=(scalar: Double): Variable = varOp(this() -= scalar, "-=", scalar.toString)

  def /=(other: Variable): Variable = varOp(this() /= other(), "/=", other.name)

  def /=(scalar: Double): Variable = varOp(this() /= scalar, "/=", scalar.toString)

  def *=(other: Variable): Variable = varOp(this() *= other(), "*=", other.name)

  def *=(scalar: Double): Variable = varOp(this() *= scalar, "*=", scalar.toString)


  /**
   * Applies a masking function on the current variable array
   */
  def mask(f: Double => Boolean, maskVal: Double = 0.0): Variable = {
    varStatOp(this().mask(f, maskVal), "maskf", maskVal.toString)
  }

  /**
   * Sets the default mask value "for the particular array being used.
   */
  def setMask(num: Double): Variable = {
    array.setMask(num)
    this
  }

  /**
   * Masks the current variable array by preserving values
   * less than or equal to num.
   */
  def <=(num: Double): Variable = varOp(this() <= num, "<=", num.toString)

  /**
   * Masks the current variable array by preserving values
   * greater than or equal to num.
   */
  def >=(num: Double): Variable = varOp(this() >= num, ">=", num.toString)

  /**
   * Masks the current variable array by preserving values
   * less than to num.
   */
  def <(num: Double): Variable = varOp(this() < num, "<", num.toString)

  /**
   * Masks the current variable array by preserving values
   * greater than num.
   */
  def >(num: Double): Variable = varOp(this() > num, ">", num.toString)

  /**
   * Masks the current variable array by preserving values
   * not equal to num.
   */
  def !=(num: Double): Variable = varOp(this() != num, "!=", num.toString)

  /**
   * Masks the current variable array by preserving values
   * equal to num.
   */
  def :=(num: Double): Variable = varOp(this() := num, ":=", num.toString)


  /**
   * Statistical operations
   */

  /**
   * Computes the mean along the given axis of the variable in use.
   *
   * @param axis the axis to take the mean along (can be more than one axis)
   * @return the reduced array with means taken along the specified dimension(s)
   */
  def mean(axis: Array[Int]): Variable = {
    varStatOp(this().mean(axis: _*), "mean", axis.toList.toString)
  }

  /**
   * Computes and returns the array broadcasted to
   * the specified shape requirements.
   *
   * @param shape the new shape to be broadcasted to
   * @return
   */
  def broadcast(shape: Array[Int]): Variable = {
    varStatOp(this().broadcast(shape), "broadcast", shape.toList.toString)
  }

  /**
   * Detrends along a series of axis specified.
   * Currently only detrends along the first axis specified.
   *
   * TODO :: Support Detrending along multiple axis
   * @param axis the series of axis to detrend alon
   * @return
   */
  def detrend(axis: Array[Int]): Variable = {
    varStatOp(this().detrend(axis(0)), "detrend", axis.toList.toString)
  }

  def std(axis: Array[Int]): Variable = {
    varStatOp(this().std(axis: _*), "std", axis.toList.toString)
  }

  def skew(axis: Array[Int]): Variable = {
    varStatOp(this().skew(axis: _ *), "skew", axis.toList.toString)
  }

  /**
   * Returns a block averaged tensor where the blocks are squares with
   * dimensions blockInt.
   */
  def reduceResolution(blockInt: Int, invalid: Double = Double.NaN): Variable = {
    val reduced = MCCOps.reduceResolution(this(), blockInt, invalid)
    varStatOp(reduced, "reduceResolution", (blockInt, blockInt).toString)
  }

  /**
   * ------------------------------ Matrix Operations ---------------------------------
   * The following functions are Matrix Operations specific to SciSpark and it's goals.
   */

  /**
   * Returns a block averaged matrix where the blocks are rectangles with dimensions
   * rowblockSize X colblockSize.
   */
  def reduceRectangleResolution(rowblockSize: Int, colblockSize: Int, invalid: Double = Double.NaN): Variable = {
    val reduced = MCCOps.reduceRectangleResolution(this(), rowblockSize, colblockSize, invalid)
    varStatOp(reduced, "reduceResolution", (rowblockSize, colblockSize).toString)
  }


  /**
   * Creates a copy of the variable
   *
   * @return
   */
  def copy(): Variable = new Variable(name, dataType, array.copy, attributes.clone(), dims)

  override def clone(): AnyRef = this.copy()

  /**
   * It should print just the same or similar to how
   * variables are printed in Netcdf python.
   * The attributes are printed in lexicographic order
   * e.g.
   *
   * float32 ch4(time, latitude, longitude)
   * comments: Unknown1 variable comment
   * grid_name: grid01
   * grid_type: linear
   * level_description: Earth surface
   * long_name: IR BT (add 75 to this value)
   * missing_value: 330.0
   * units:
   * time_statistic: instantaneous
   * current shape = (1238, 4125)
   *
   */
  override def toString: String = {
    val dimensionString = "(" + dims.map(_._1).reduce((a, b) => a + ", " + b) + ")"
    val header = dataType + " " + name + dimensionString + "\n"
    val shapeString = shape().map(_.toString).reduce((a, b) => a + ", " + b)
    val footer = "current shape = (" + shapeString + ")\n"
    val body = new StringBuilder()
    body.append(header)
    for ((k, v) <- attributes.toSeq.sorted) {
      body.append("\t" + k + ": " + v + "\n")
    }
    body.append(footer)
    body.toString()
  }

  override def equals(any: Any): Boolean = {
    val variable = any.asInstanceOf[Variable]
    this.name == variable.name &&
      this.dataType == variable.dataType &&
      this.array == variable.array &&
      this.attributes == variable.attributes
  }

  override def hashCode(): Int = super.hashCode()

  private def varOp(abstractTensor: AbstractTensor, op: String, otherName: String): Variable = {
    val newName = "(" + this.name + " " + op + " " + otherName + ")"
    op match {
      case "+" | "-" | "/" | "*" | "**" =>
        new Variable(newName, dataType, abstractTensor, attributes.clone, dims)
      case "<" | ">" | "<=" | ">=" | "!=" | ":=" =>
        new Variable(newName, dataType, abstractTensor, attributes.clone, dims)
      case "+=" | "-=" | "/=" | "*=" =>
        this.name = newName
        this
      case _ => throw new Exception(op + "is not a Binary Variable Operation")
    }
  }

  private def varStatOp(abstractTensor: AbstractTensor, op: String, param: String): Variable = {
    val newName = op + "(" + this.name + "," + param + ")"
    new Variable(newName, dataType, abstractTensor, attributes.clone, dims)
  }

  private def arrayOp(abstractTensor: AbstractTensor, op: String) = {
    val newName = this.name + "[" + op + "]"
    new Variable(newName, dataType, abstractTensor, attributes.clone, dims)
  }

}

