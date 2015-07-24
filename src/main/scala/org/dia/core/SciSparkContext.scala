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

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.dia.Constants._
import org.dia.partitioners.sPartitioner
import sPartitioner._
import org.dia.loaders.NetCDFLoader._

import scala.io.Source
/**
 * SciSpark contexts extends the existing SparkContext function.
 * However there are many private functions within SparkContext
 * that are useful for catching unwanted calls. Such as
 * executing one of the functions after the SparkContext has been stopped.
 *
 * TODO :: Should we extend SparkContext or modify a copy of SparkContext
 */
class SciSparkContext(val conf : SparkConf) {

  val sparkContext = new SparkContext(conf)
  sparkContext.setLocalProperty(ARRAY_LIB, ND4J_LIB)

  def this(url : String, name : String){
    this(new SparkConf().setMaster(url).setAppName(name))
  }

  def setLocalProperty(key : String, value : String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def getConf : SparkConf = sparkContext.getConf
  /**
   * Constructs an sRDD from a file of openDap URL's pointing to NetCDF datasets.
   *
   * TODO :: Support for reading more than one variable
   * TODO :: Properly integrate minimum partitioning
   *
   * @param path Path to a file containing a list of OpenDap URLs
   * @param varName the variable name to search for
   * @param minPartitions the minimum number of partitions
   * @return
   */
    @transient def OpenDapURLFile(path: String,
                       varName : String,
                       minPartitions: Int = sparkContext.defaultMinPartitions) : sRDD[sciTensor] = {

      val datasetUrls = Source.fromFile(path).mkString.split("\n").toList
      new sRDD[sciTensor](sparkContext, datasetUrls, varName, loadNetCDFNDVars, mapOneUrlToOneTensor)
    }

    @transient def OpenPath(path: String, varName : String) : sRDD[sciTensor] = {
      val datasetPaths = List(path)
      new sRDD[sciTensor](sparkContext, datasetPaths, varName, loadNetCDFNDVars, mapSubFoldersToFolders)
    }

}
