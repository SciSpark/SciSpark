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

import org.dia.tensors.{AbstractTensor, Nd4jTensor}
import org.dia.utils.NetCDFUtils

/**
 * A Variable is a logical container for data.
 * It has a dataType, a shape, and a set of Attributes.
 *
 * The data is a multidimensional array.
 * Data access is done through the apply() method,
 * which returns the multidimensional array as an AbstractTensor.
 */
class Variable(val name: String,
               val dataType: String,
               val array: AbstractTensor,
               val attributes: mutable.LinkedHashMap[String, String],
               val dims: mutable.LinkedHashMap[String, Int]) extends Serializable {

  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def this(name: String,
           dataType: String,
           array: AbstractTensor,
           attr: TraversableOnce[(String, String)],
           dims: mutable.LinkedHashMap[String, Int]) {
    this(name, dataType, array, new mutable.LinkedHashMap[String, String] ++= attr, dims)
  }

  def this(name: String,
           daaType: String,
           array: AbstractTensor,
           attr: java.util.List[Attribute],
           dims: mutable.LinkedHashMap[String, Int]) {
    this(name, daaType, array, attr.asScala.map(p => NetCDFUtils.convertAttribute(p)), dims)
  }

  def this(name: String,
           dataType: String,
           array: Array[Double],
           shape: Array[Int],
           attr: java.util.List[Attribute],
           dims: TraversableOnce[(String, Int)]) {
    this(name,
         dataType,
         new Nd4jTensor(array, if (shape.length > 1) shape else Array(1) ++ shape),
         attr,
         new mutable.LinkedHashMap[String, Int]() ++= dims)
  }


  def this(name: String, nvar: ucar.nc2.Variable) {
    this(name, nvar.getDataType.toString,
      NetCDFUtils.getArrayFromVariable(nvar),
      nvar.getShape,
      nvar.getAttributes,
      nvar.getDimensions.asScala.map(p => (p.getFullName, p.getLength)))
  }

  def this(nvar: ucar.nc2.Variable) {
    this(nvar.getFullName, nvar)
  }

  def this(name: String, array: AbstractTensor) {
    this(name,
         "Double64",
         array,
         new mutable.LinkedHashMap[String, String],
         new mutable.LinkedHashMap[String, Int])
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

  def shape(): Array[Int] = array.shape

  def data(): Array[Double] = array.data

  /**
   * Creates a copy of the variable
   *
   * @return
   */
  def copy(): Variable = new Variable(name, dataType, array.copy, attributes.clone(), dims.clone())

  override def clone(): AnyRef = this.copy()

  /**
   * It should print just the same or similar to how
   * variables are printed in Netcdf python.
   * e.g.
   *
   * float32 ch4(time, latitude, longitude)
   * comments: Unknown1 variable comment
   * long_name: IR BT (add 75 to this value)
   * units:
   * grid_name: grid01
   * grid_type: linear
   * level_description: Earth surface
   * time_statistic: instantaneous
   * missing_value: 330.0
   * current shape = (1238, 4125)
   *
   */
  override def toString: String = {
    val dimensionString = dims.keys.toString.replace("Set", "")
    val header = dataType + " " + name + dimensionString + "\n"
    val footer = "current shape = " + shape().toList + "\n"
    val body = new StringBuilder()
    body.append(header)
    for ((k, v) <- attributes) {
      body.append("\t" + k + ": " + v + "\n")
    }
    body.append(footer.replace("List", ""))
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
}

