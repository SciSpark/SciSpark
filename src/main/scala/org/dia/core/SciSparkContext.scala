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

import org.apache.spark.SparkContext
import org.dia.NetCDFUtils
//import org.dia.core.singleUrl.sciNd4jRDD
import org.nd4j.linalg.api.ndarray.INDArray

import scala.io.Source
/**
 * SciSpark contexts extends the existing SparkContext function.
 * However there are many private functions within SparkContext
 * that are useful for catching unwanted calls. Such as
 * executing one of the functions after the SparkContext has been stopped.
 *
 * TODO :: Should we extend SparkContext or modify a copy of SparkContext
 * Created by rahulsp on 7/8/15.
 */
class SciSparkContext(master : String, appName : String) extends SparkContext(master, appName) {

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
//    def OpenDapURLFile(path: String,
//                       varName : String,
//                       minPartitions: Int = defaultMinPartitions) : sciNd4jRDD[INDArray] = {

//      val datasetUrls = Source.fromFile(path).mkString.split("\n").toList
//      new sciNd4jRDD[INDArray](this, datasetUrls, varName)
//    null
//    }

}
