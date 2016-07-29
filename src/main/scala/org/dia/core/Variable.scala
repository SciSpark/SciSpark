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
import org.dia.tensors.AbstractTensor
import org.dia.utils.NetCDFUtils
import ucar.nc2.Attribute
import scala.collection.{TraversableOnce, mutable}

class Variable(val name : String,
               val numericType : String,
               val array : AbstractTensor,
               val attributes : mutable.HashMap[String, String]) extends Serializable {

  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def this(name: String, numericType: String, array : AbstractTensor, attr : TraversableOnce[(String, String)]){
    this(name, numericType, array, new mutable.HashMap[String, String] ++= attr)
  }

  def this(name: String, numericType: String, array : AbstractTensor, attr : Array[Attribute]){
    this(name, numericType, array, attr.map(p => NetCDFUtils.convertAttribute(p)))
  }

  def this(name: String, numericType: String, array : AbstractTensor, attr : java.util.List[Attribute]){
    this(name, numericType, array, attr.toArray.map(p => p.asInstanceOf[Attribute]))
  }

  def this(name: String, array : AbstractTensor){
    this(name, "Double64", array, new mutable.HashMap[String, String])
  }

  def this(array : AbstractTensor) {
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
   *    variable("attribute")
   *
   * @param key the attribute name
   * @return the attribute value
   */
  def apply(key : String) : String = attributes(key)

  def shape(): Array[Int] = array.shape
  def data() : Array[Double] = array.data

  /**
   * Creates a copy of the variable
   * @return
   */
  def copy() : Variable = new Variable(name, numericType, array.copy, attributes.clone())

  /**
   * It should print just the same or similar to how
   * variables are printed in Netcdf python.
   * e.g.
   *
   * float32 ch4(time, latitude, longitude)
   *    comments: Unknown1 variable comment
   *    long_name: IR BT (add 75 to this value)
   *    units:
   *    grid_name: grid01
   *    grid_type: linear
   *    level_description: Earth surface
   *    time_statistic: instantaneous
   *    missing_value: 330.0
   * current shape = (1238, 4125)
   *
   */
  override def toString: String = {
    val header = numericType + " " + name + "\n"
    val footer = "current shape = " + shape().toList + "\n"
    val body = new StringBuilder()
    body.append(header)
    for((k,v) <- attributes){
      body.append("\t" + k + ": " + v + "\n" )
    }
    body.append(footer.replace("List", ""))
    body.toString()
  }

  override def equals(any: Any) : Boolean = {
    val variable = any.asInstanceOf[Variable]
    this.name == variable.name &&
      this.numericType == variable.numericType &&
      this.array == variable.array &&
      this.attributes == variable.attributes
  }

}

