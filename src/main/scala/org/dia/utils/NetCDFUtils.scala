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
package org.dia.utils

import java.net.URL

import scala.language.implicitConversions

import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import ucar.ma2
import ucar.ma2.DataType
import ucar.nc2.{Attribute, NetcdfFile, Variable}
import ucar.nc2.dataset.NetcdfDataset

import org.dia.HDFSRandomAccessFile

/**
 * Utilities to read a NetCDF by URL or from HDFS.
 *
 * Note that we use -9999.0 instead of NaN to indicate missing values.
 */
object NetCDFUtils extends Serializable {

  // Class logger
  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Adds http credentials which is then registered by any function
   * using ucar's httpservices. Namely, if you are reading any opendap file
   * that requires you to enter authentication credentials in the browser.
   * Some datasets (like those hosted by gesdisc) require http credentials
   * for authentiation and access.
   *
   * @param url the http url to connect to
   * @param username the username field
   * @param password the password field
   */
  def setHTTPAuthentication(url: String, username: String, password: String): Unit = {
    val urlobj = new URL(url)
    val authscope = new AuthScope(urlobj.getHost, urlobj.getPort)
    val credentials = new UsernamePasswordCredentials(username, password)
    ucar.httpservices.HTTPSession.setGlobalCredentials(authscope, credentials)
  }

  /**
   * Extracts a variable's data from a NetCDF.
   *
   * If the variable is not found then an Array with the element 0.0
   * is returned with shape (1,1). Note that all dimensions of size 1 are eliminated.
   * For example the array shape (21, 5, 1, 2) is reduced to (21, 5, 2) since the 3rd dimension
   * only ranges over a single value.
   *
   * @param netcdfFile The NetCDF file.
   * @param variable   The variable whose data we want to extract.
   * @return Data and shape arrays.
   */
  def netCDFArrayAndShape(netcdfFile: NetcdfDataset, variable: String): (Array[Double], Array[Int]) = {
    val searchVariableArray = getNetCDFVariableArray(netcdfFile, variable)
    if (searchVariableArray == null) {
      LOG.error("Variable '%s' not found. Can't create array. Returning empty array.".format(variable))
      return (Array(0.0), Array(1, 1))
    }
    val nativeArray = convertMa2Arrayto1DJavaArray(searchVariableArray)
    val shape = searchVariableArray.getShape
    (nativeArray, shape)
  }

  /**
   * Converts the native ma2.Array from the NetCDF library
   * to a one dimensional Java Array of Doubles.
   * Extracts the 1d Java array and converts the elements to a Double Type.
   *
   * TODO:: The array libraries need to be able to accept different types of numerics.
   * Nd4j only accepts Floats and Doubles.
   * Breeze accepts Ints, Floats, and Doubles.
   */
  def convertMa2Arrayto1DJavaArray(ma2Array: ma2.Array): Array[Double] = {
    ma2Array.getDataType match {
      case DataType.INT => ma2Array.copyTo1DJavaArray.asInstanceOf[Array[Int]].map(_.toDouble)
      case DataType.SHORT => ma2Array.copyTo1DJavaArray().asInstanceOf[Array[Short]].map(_.toDouble)
      case DataType.FLOAT => ma2Array.copyTo1DJavaArray.asInstanceOf[Array[Float]].map(_.toDouble)
      case DataType.DOUBLE => ma2Array.copyTo1DJavaArray.asInstanceOf[Array[Double]]
      case DataType.LONG => ma2Array.copyTo1DJavaArray().asInstanceOf[Array[Long]].map(_.toDouble)
      case badType => throw new Exception("Can't convert ma2.Array[" + badType + "] to numeric array.")
    }
  }

  /**
   * Extracts a variable's data from a NetCDF file as an M2 array.
   *
   * @param netcdfFile the NetcdfDataSet to read from
   * @param variable   the variable whose array we want to extract
   * @todo Ask why OCW hardcodes the latitude and longitude names
   */
  def getNetCDFVariableArray(netcdfFile: NetcdfDataset, variable: String): ma2.Array = {
    var searchVariable: ma2.Array = null
    try {
      if (netcdfFile == null) throw new IllegalStateException("NetCDFDataset was not loaded")
      val netcdfVal = netcdfFile.findVariable(variable)
      if (netcdfVal == null) throw new IllegalStateException("Variable '%s' was not loaded".format(variable))
      searchVariable = netcdfVal.read()
    } catch {
      case ex: Exception =>
        LOG.error("Variable '%s' not found when reading source %s.".format(variable, netcdfFile))
        LOG.info("Variables available: " + netcdfFile.getVariables)
        throw ex
    }
    searchVariable
  }

  /**
   * Loads a NetCDF Dataset from a URL.
   */
  def loadNetCDFDataSet(url: String): NetcdfDataset = {
    NetcdfDataset.setUseNaNs(false)
    val dataset = try {
      NetcdfDataset.openDataset(url)
    } catch {
      case e: java.io.IOException =>
        LOG.error("Couldn't open dataset %s".format(url))
        throw e
      case ex: Exception =>
        LOG.error("Something went wrong while reading %s".format(url))
        throw ex
    }
    LOG.info("Succesfully downloaded netcdf file from :" + url)
    dataset
  }

  /**
   * Loads a NetCDF Dataset from HDFS.
   *
   * @param dfsUri     HDFS URI(eg. hdfs://master:9000/)
   * @param location   File path on HDFS
   * @param bufferSize The size of the buffer to be used
   */
  def loadDFSNetCDFDataSet(dfsUri: String, location: String, bufferSize: Int): NetcdfDataset = {
    NetcdfDataset.setUseNaNs(false)
    try {
      val raf = new HDFSRandomAccessFile(dfsUri, location, bufferSize)
      new NetcdfDataset(NetcdfFile.open(raf, location, null, null))
    } catch {
      case e: java.io.IOException =>
        LOG.error("Couldn't open dataset %s%s".format(dfsUri, location))
        throw e
      case ex: Exception =>
        LOG.error("Something went wrong while reading %s%s".format(dfsUri, location))
        throw ex
    }
  }

  /**
   * Gets the size of a dimension of a NetCDF file.
   */
  def getDimensionSize(netcdfFile: NetcdfDataset, rowDim: String): Int = {
    var dimSize = -1
    val it = netcdfFile.getDimensions.iterator()
    while (it.hasNext) {
      val d = it.next()
      if (d.getShortName.equals(rowDim)) dimSize = d.getLength
    }
    if (dimSize < 0) throw new IllegalStateException("Dimension does not exist!!!")
    dimSize
  }

  /**
   * Given an attribute, converts it into a
   * key value String pair.
   * The attribute data type is checked.
   * If it is not a string then it is number and
   * is converted to a String.
   *
   * @param attribute the netCDF attribute
   * @return (attribute name, attribute value)
   */
  def convertAttribute(attribute: Attribute): (String, String) = {
    val key = attribute.getFullName.replace("\\", "")
    val value = attribute.getDataType match {
      case DataType.STRING => attribute.getStringValue
      case _ => attribute.getNumericValue().toString
    }
    (key, value)
  }

  /**
   * Extracts the flattened double array from a netCDF Variable
   *
   * @param variable the netCDF variable
   * @return the flattened Double Array
   */
  def getArrayFromVariable(variable: Variable): Array[Double] = {
    var searchVariable: ma2.Array = null
    if (variable == null) throw new IllegalStateException("Variable '%s' was not loaded".format(variable))
    try {
      searchVariable = variable.read()
    } catch {
      case ex: Exception =>
        LOG.error("Variable '%s' not found when reading source %s.".format(variable))
        LOG.info("Variables available: " + variable)
        LOG.error(ex.getMessage)
    }
    convertMa2Arrayto1DJavaArray(searchVariable)
  }

}
