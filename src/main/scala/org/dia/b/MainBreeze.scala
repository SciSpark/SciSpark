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
package org.dia.b

import breeze.linalg.{DenseMatrix, sum}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.dia.Constants._
import org.dia.NetCDFUtils
import ucar.ma2
import ucar.nc2.dataset.NetcdfDataset

import scala.collection.mutable.MutableList
import scala.language.implicitConversions

/**
 * Functions needed to perform operations with Breeze
 */
object MainBreeze {


  def createSciRdd(url: String, variable: String) = {
    // partition by time
//    val sciRdd = org.apache.spark.rdd.RDD[DenseMatrix]
  }

  def main(args : Array[String]) : Unit = {
    val TextFile = "TestLinks"
    var cores = Runtime.getRuntime().availableProcessors() - 1;
    //TODO the number of threads should be configured at cluster level
    val conf = new SparkConf().setAppName("L").setMaster("local[" + cores + "]")
    val sparkContext = new SparkContext(conf)
    val urlRDD = sparkContext.textFile(TextFile).repartition(cores)
    // depending on the file name or the data set we can create different rdds
    // NOTE: if partitioning by time defined in the file name, then the whole data set is the rdd
    // and the partition comes from the file name itself
    val sciRDD = urlRDD.map(url => createSciRdd(url, DATASET_VARS.get("trmm").toString))
    // print content
    //sciRDD.map(degres_east => println(value))
    println(sciRDD.count())

//    val HighResolutionArray = urlRDD.map(url => getNd4jNetCDFVars(url, DATASET_VARS.get("ncml").toString))
//    val nanoAfter = System.nanoTime()
//    val LowResolutionArray = HighResolutionArray.map(largeArray => Nd4jReduceResolution(largeArray, 5)).collect
//    LowResolutionArray.map(array => println(array))
  }

}

