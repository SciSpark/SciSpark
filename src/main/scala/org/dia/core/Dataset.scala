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

import scala.collection.JavaConverters._
import scala.collection.mutable

import ucar.nc2.Attribute
import ucar.nc2.dataset

import org.dia.utils.NetCDFUtils

/**
 * Dataset is a logical container for variable arrays.
 * A NetcdfDataset wraps a LinkedHashMap of variables and
 * a LinkedHashMap of attributes.
 */
class Dataset(val variables: mutable.LinkedHashMap[String, Variable],
              val attributes: mutable.LinkedHashMap[String, String]) extends Serializable{

  def this(vars : Traversable[(String, Variable)], attr : Traversable[(String, String)]) {
    this(new mutable.LinkedHashMap[String, Variable] ++= vars,
         new mutable.LinkedHashMap[String, String] ++= attr)
  }

  def this(vars : Iterable[ucar.nc2.Variable], attr : Iterable[Attribute]) {
    this(vars.map(p => (p.getFullName, new Variable(p))), attr.map(p => NetCDFUtils.convertAttribute(p)))
  }

  def this(nvar: dataset.NetcdfDataset) {
    this(nvar.getVariables.asScala, nvar.getGlobalAttributes.asScala)
  }

  /**
   * Writes attribute in the form of key-value pairs
   */
  def insertAttributes(metaDataVar: (String, String)*): Unit = {
    insertAttributes(metaDataVar)
  }

  def insertAttributes(metaDataVars: Traversable[(String, String)]): Unit = {
    attributes ++= metaDataVars
  }

  /**
   * Writes variables in the form of key-value pairs
   */
  def insertVariable(variables: (Variable)*): Unit = {
    for (v <- variables) this.variables += ((v.name, v))
  }

  /**
   * Access variables.
   * In Python's netcdf Dataset, variables can be accessed as
   * members of classes like so:
   *    dataset['variable']
   * In scala, variables can be access with the
   * apply function like so:
   * dataset("variable")
   *
   * @param key the variable name
   * @return the variable
   */
  def apply(key: String): Variable = variables(key)

  /**
   * Access attribute values.
   * In Python's netcdf variable, attributes can be accessed as
   * members of classes like so:
   *    variable.attribute1
   * In scala we can't do that so we access attributes in
   * datasets like so:
   * dataset.attr("attribute")
   *
   * @param key the attribute name
   * @return the attribute value
   */
  def attr(key: String): String = attributes(key)

  /**
   * Creates a clone of the variable
   *
   * @return
   */
  def copy(): Dataset = new Dataset(variables.clone(), attributes.clone())

  override def toString: String = {
    val header = "root group ...\n"
    val variableString = variables.map({case (str, varb) => varb.dataType + " " + str})
    val footer = "\tvariables: " + variableString + "\n"
    val body = new StringBuilder()
    body.append(header)
    for ((k, v) <- attributes) {
      body.append("\t" + k + ": " + v + "\n")
    }
    body.append(footer.replaceAll("(ArrayBuffer|\\(|\\))", ""))
    body.toString()
  }

  override def equals(any: Any): Boolean = {
    val dataset = any.asInstanceOf[Dataset]
    dataset.attributes == this.attributes &&
    dataset.variables == this.variables
  }

  override def hashCode(): Int = super.hashCode()
}
