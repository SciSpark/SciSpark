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
package org.dia.n

import org.apache.spark.{SparkConf, SparkContext}
import org.dia.TRMMUtils.Constants
import org.dia.b.TrmmHourlyRDD

import scala.language.implicitConversions

/**
 * * Functions needed to perform operations with Nd4j
 */
object Main {

  def main(args : Array[String]) : Unit = {
    val TextFile = "TestLinks"
    val cores = Runtime.getRuntime.availableProcessors - 1
    //TODO the number of threads should be configured at cluster level
    val conf = new SparkConf().setAppName("L").setMaster("local[" + cores + "]")
    val sc = new SparkContext(conf)
    val urlRDD = sc.textFile(TextFile).repartition(cores)
    // depending on the file name or the data set we can create different rdds
    // NOTE: if partitioning by time defined in the file name, then the whole data set is the rdd
    // and the partition comes from the file name itself
    //val trmmRDD = new TrmmHourlyRDD(sc, Constants.TRMM_HOURLY_URL, 1997, 1997)
    // print content
//        val HighResolutionArray = urlRDD.map(url => Nd4jFuncs.getNd4jNetCDFVars(url, DATASET_VARS.get("ncml").toString))
//        val nanoAfter = System.nanoTime()
//        val LowResolutionArray = HighResolutionArray.map(largeArray => Nd4jReduceResolution(largeArray, 5)).collect
//        LowResolutionArray.map(array => println(array))
  }
}

