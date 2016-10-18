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

import java.io.{File, FileWriter, PrintWriter, Writer}
import java.nio

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.rdd.RDD

import org.dia.core.SciDataset


/**
 * Utilities to assist with using the SciSpark frontend interface
 */

object NotebookUtils {

  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Method to extract some user configs from the infrastructure stack
   * @param  ouputDir The directory output data from the run is written to
   * @return hdfsURL  The path to the directory using the hdfs:///
   */
  def getHdfsURL(outputDir: String): String = {
    val fileSepStr = System.getProperty("file.separator")
    val x = outputDir.split(System.getProperty("file.separator"))
    val hdfsURL = "hdfs:" + fileSepStr + fileSepStr +
      x.slice(3, x.length).mkString(System.getProperty("file.separator"))
    hdfsURL
  }

  /**
   * Method to get the username
   */
  def getUser(): String = {
    UserGroupInformation.getCurrentUser().getUserName()
  }

  /**
   * Method to extract the variable from a SciDataset into an Java List
   * @param sRDD      An RDD of sciDatasets
   * @param varname   The variable name to be extracted
   * @param identifer Optional. The name of an SciDataset attribute that can be used to uniquely
   *                    identify the order of data compared to that from the others in the sRDD.
   *                    Assumes value can be converted to an Integer.
   * @return a Java List containing a Java arrayBuffer of the data and a Scala array of the shape,
   *         and opitonally an attribute
   */
  def getVariableArray(
      sRDD: RDD[SciDataset],
      varname: String,
      identifer: String = null): RDD[java.util.ArrayList[Any]] = {
    val jList = sRDD.map(p => {
      val javaList = new java.util.ArrayList[Any]()
      val scalaBytebuffer = java.nio.ByteBuffer.allocate(8 * p(varname).data.length)
      scalaBytebuffer.asDoubleBuffer.put(java.nio.DoubleBuffer.wrap(p(varname).data()))
      javaList.add(scalaBytebuffer.array)
      javaList.add(p(varname).shape())
      if (identifer != null) {
        javaList.add(java.lang.Integer.valueOf(p.attr(identifer).toInt))
      }
      else {
        javaList.add(java.lang.String.valueOf(p.datasetName))
      }
      javaList
    })
    jList
  }
}
