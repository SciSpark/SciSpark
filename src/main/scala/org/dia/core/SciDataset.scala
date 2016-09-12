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

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import ucar.ma2.{Array, DataType}
import ucar.nc2.{dataset, Attribute, NetcdfFileWriter}

import org.dia.utils.NetCDFUtils

/**
 * Dataset is a logical container for variable arrays.
 * A NetcdfDataset wraps a LinkedHashMap of variables and
 * a LinkedHashMap of attributes.
 *
 * The LinkedHashMap preserves insertion order for iteration purposes.
 */
class SciDataset(val variables: mutable.HashMap[String, Variable],
                 val attributes: mutable.HashMap[String, String],
                 var datasetName: String) extends Serializable{

  def this(vars : Traversable[(String, Variable)], attr : Traversable[(String, String)], datasetName : String) {
    this(mutable.HashMap[String, Variable]() ++= vars,
         mutable.HashMap[String, String]() ++= attr,
         datasetName)
  }

  def this(vars : Iterable[ucar.nc2.Variable], attr : Iterable[Attribute], datasetName : String) {
    this(vars.map(p => (p.getFullName, new Variable(p))),
         attr.map(p => NetCDFUtils.convertAttribute(p)),
         datasetName)
  }

  def this(nvar: dataset.NetcdfDataset) {
    this(nvar.getVariables.asScala, nvar.getGlobalAttributes.asScala, nvar.getLocation.split("/").last)
  }

  def this(nvar: dataset.NetcdfDataset, vars: List[String]) {
    this(vars.map(vr => nvar.findVariable(vr)), nvar.getGlobalAttributes.asScala, nvar.getLocation.split("/").last)
  }

  def this(datasetName : String) {
    this(mutable.HashMap[String, Variable](), mutable.HashMap[String, String](), datasetName)
  }

  /**
   * Extract all the dimension names from the variables
   * Convert them into a string representation e.g.
   *
   * row(400), cols(1440)
   *
   * Flattens all the dimensions amongst the variables
   * and keep the distinct dimensions.
   */
  def globalDimensions() : List[String] = {
    variables.valuesIterator.map(variable =>
      variable.dims.map({
        case(dimName, length) => dimName + "(" + length + ")"
      })
    ).flatten.toList.distinct
  }

  /**
   * Writes attribute in the form of key-value pairs
   *
   * @param metaDataVar tuple(s) of (attribute name, attribute value)
   * @return The modified SciDataset with added attributes
   */
  def insertAttributes(metaDataVar: (String, String)*): SciDataset = {
    insertAttributes(metaDataVar)
    this
  }

  /**
   * Writes attribute in the form of key-value pairs in a collection.
   * @param metaDataVars collection of tuple(s) of (attribute name, attribute value)
   * @return The modified SciDataset with added attributes
   */
  def insertAttributes(metaDataVars: Traversable[(String, String)]): SciDataset = {
    attributes ++= metaDataVars
    this
  }

  /**
   * Writes variables in the form of key-value pairs
   *
   * @param variables collection of tuple(s) of (variable name, variable object)
   * @return The modified SciDataset with added attributes
   */
  def insertVariable(variables: (String, Variable)*): SciDataset = {
    for ((k, v) <- variables) this.variables += ((k, v))
    this
  }

  /**
   * Writes a new variable to the hashmap.
   * It is recommended to use the update function instead
   * which enables you to use the "=" operator to insert new variables.
   * @param key name of variable
   * @param value the variable object ot insert
   * @return the current SciDataset
   */
  def insertVariable(key: String, value: Variable): SciDataset = {
    this.variables(key) = value
    this
  }

  /**
   * Assigns a new name to the Dataset
   * @param newName name to assign Dataset to
   * @return the renamed SciDataset
   */
  def setName(newName : String): SciDataset = {
    datasetName = newName
    this
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
   * Updates the SciDataset by inserting a new Variable
   * for the given key.
   *
   * Usage sciD("key") = variable
   * @param key the variable key to rename to
   * @param variable the variable
   * @return the modifed SciDataset
   */
  def update(key: String, variable: Variable): SciDataset = {
    variables(key) = variable
    this
  }

  /**
   * Updates the SciDataset by inserting a new attribute
   * for the given key.
   *
   * Usage sciD("key") = attributeString
   * @param key the variable key to rename to
   * @param attribute the attribute name
   * @return the modified SciDataset
   */
  def update(key: String, attribute: String): SciDataset = {
    attributes(key) = attribute
    this
  }

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
   * Writes the contents of SciDataset to a NetcdfFile.
   *
   * @param name Optional : The name of the netcdf file. If no name is specified, then
   *             the current datasetName is used. Note that the function does not
   *             append ".nc" by default and so must be included in the name.
   * @param path Optional : The directory where this file will be written to.
   *             By default it is written to the current directory.
   */
  def writeToNetCDF(name: String = datasetName, path: String = ""): Unit = {
    val writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, path + name, null)
    val globalDimensionMap = mutable.HashMap[String, ucar.nc2.Dimension]()
    val netcdfKeyValue = variables.map {
      case (key, variable) =>
        val dims = new util.ArrayList[ucar.nc2.Dimension]()
        for ((dimName, length) <- variable.dims) {
          val newDim = globalDimensionMap.getOrElseUpdate(dimName, writer.addDimension(null, dimName, length))
          dims.add(newDim)
        }
        val varT = writer.addVariable(null, key, ucar.ma2.DataType.FLOAT, dims)
        varT.addAll(variable.attributes.map(p => new Attribute(p._1, p._2)).asJava)
        val dataOut = Array.factory(DataType.DOUBLE, variable.shape(), variable.data())
        (varT, dataOut)
    }
    for ((key, attribute) <- attributes) {
      writer.addGroupAttribute(null, new Attribute(key, attribute))
    }

    writer.create()
    for ((variable, array) <- netcdfKeyValue) writer.write(variable, array)
    writer.close()
  }

  /**
   * Creates a clone of the SciDataset
   *
   * @return
   */
  def copy(): SciDataset = {
    /**
     * Hashmaps by default in scala do not do a deep clone.
     * Cloning the variable hashmap only copies the references
     * not the actual objects.
     *
     * Instead each variable in the hashmap is cloned.
     */
    val clonedVariables = variables.map({case (name, variable) => (name, variable.copy())})
    new SciDataset(clonedVariables, attributes.clone.toSeq, datasetName)
  }

  override def toString: String = {
    val header = datasetName + "\nroot group ...\n"
    val dimensionString = "\tdimensions(sizes): " + globalDimensions().toString + "\n"
    val variableString = variables.map({case (str, varb) => varb.dataType + " " + str + varb.dims.map(_._1)})
    val footer = "\tvariables: " + variableString + "\n"
    val body = new StringBuilder()
    body.append(header)
    for ((k, v) <- attributes) {
      body.append("\t" + k + ": " + v + "\n")
    }
    body.append(dimensionString.replace("List", ""))
    body.append(footer.replaceAll("(ArrayBuffer|List)", ""))
    body.toString()
  }

  override def equals(any: Any): Boolean = {
    val dataset = any.asInstanceOf[SciDataset]
    dataset.datasetName == this.datasetName &&
    dataset.attributes == this.attributes &&
    dataset.variables == this.variables
  }

  override def hashCode(): Int = super.hashCode()
}
