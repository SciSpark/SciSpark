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

import org.apache.spark.storage.StorageLevel
import org.dia.Constants._
import org.dia.TRMMUtils.HourlyTrmmUrlGenerator
import org.dia.loaders.NetCDFLoader._
import org.dia.partitioners.sPartitioner._
import org.dia.partitioners.sTrmmPartitioner._
import org.scalatest.FunSuite

import scala.io.Source

/**
 * Tests for creating different Rdd types.
 */
class sRDDTest extends FunSuite {

  test("ArrayLibsSanityTest") {
    val dataUrls = Source.fromFile("TestLinks").mkString.split("\n").toList
    val sc = SparkTestConstants.sc.sparkContext

    // Breeze library
    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)
    val sBreezeRdd = new sRDD[sciTensor](sc, dataUrls, List("TotCldLiqH2O_A"), loadNetCDFNDVars, mapOneUrlToOneTensor)
    val sBreezeRdd2 = new sRDD[sciTensor](sc, dataUrls, List("TotCldLiqH2O_B"), loadNetCDFNDVars, mapOneUrlToOneTensor)
    sBreezeRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    var start = System.nanoTime()
    val breezeTensors = sBreezeRdd.collect()
    var end = System.nanoTime()
    var breezeTime = (end - start) / 1000000000.0

    // Nd4j library
    sc.setLocalProperty(ARRAY_LIB, ND4J_LIB)
    val sNd4jRdd = new sRDD[sciTensor](sc, dataUrls, List("TotCldLiqH2O_A"), loadNetCDFNDVars, mapOneUrlToOneTensor)
    //    sNd4jRdd.map( e => e("var1"))

    sNd4jRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    start = System.nanoTime()
    val nd4jTensors = sNd4jRdd.collect()
    end = System.nanoTime()
    var nd4jTime = (end - start) / 1000000000.0

    // element comparison
    var flg = true
    var cnt = 0
    nd4jTensors(0).variables("TotCldLiqH2O_A").data.foreach(e => {
      if (e != breezeTensors(0).variables("TotCldLiqH2O_A").data(cnt))
        flg = false
      cnt += 1
    })

    // printing out messages
    println("BREEZE : %.6f".format(breezeTime))
    println("ND4J : %.6f".format(nd4jTime))
    println("EQUAL ELEMENTS? %b".format(flg))
  }

  test("GroupingByYearPartitioning") {
    val urls = HourlyTrmmUrlGenerator.generateTrmmDaily(1999, 2000).toList
    val sc = SparkTestConstants.sc.sparkContext
    sc.getConf.set("log4j.configuration", "resources/log4j-defaults.properties")

    // Nd4j library
    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)
    val sNd4jRdd = new sRDD[sciTensor](sc, urls, List("precipitation"), loadNetCDFNDVars, mapOneYearToManyTensorTRMM)
    val nd4jTensor = sNd4jRdd.collect()(0)
    nd4jTensor.variables("precipitation").data.foreach(e => println(e))
    assert(true)
  }

  test("GroupingByDayPartitioning") {
    val urls = HourlyTrmmUrlGenerator.generateTrmmDaily(1999, 2000).toList
    val sc = SparkTestConstants.sc.sparkContext
    sc.getConf.set("log4j.configuration", "resources/log4j-defaults.properties")

    // Nd4j library
    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)
    val sNd4jRdd = new sRDD[sciTensor](sc, urls, List("precipitation"), loadNetCDFNDVars, mapOneDayToManyTensorTRMM)
    val nd4jTensor = sNd4jRdd.collect()(0)
    println(nd4jTensor.variables("precipitation").data)
  }

  test("sampleApiTest") {
    val sc: SciSparkContext = SparkTestConstants.sc
    sc.setLocalProperty(ARRAY_LIB, ND4J_LIB)
    val variable = "TotCldLiqH2O_A"
    val nd4jRDD: sRDD[sciTensor] = sc.NetcdfFile("TestLinks", List("TotCldLiqH2O_A"))
    val mappedRdd = nd4jRDD.map(p => p)
    mappedRdd.collect()

    //    val smoothRDD: RDD[AbstractTensor] = nd4jRDD.map(p => p.variables(variable).reduceResolution(5))

    //    val collect : Array[sciTensor] = smoothRDD.map(p => p <= 241.0).collect

    //    println(collect.toList)

    assert(true)
  }

  test("BreezeRdd.basic") {
    //    val sc = SparkTestConstants.sc
    //    val datasetUrls = Source.fromFile("TestLinks").mkString.split("\n").toList
    //    val datasetMapping = datasetUrls.foreach(element => (0, element)).asInstanceOf[Map[AnyVal, Any]]
    //    val srdd = new sciBreezeRDD[DenseMatrix[Double]] (sc, datasetMapping, "TotCldLiqH2O_A")

    //    val collected = srdd.collect
    //    collected.map(p => println(p))
    //    sc.stop()
    assert(true)
  }

  test("Nd4jRdd.basic") {
    //    val sc = SparkTestConstants.sc
    //    val datasetUrls = Source.fromFile("TestLinks").mkString.split("\n").toList
    //    val datasetMapping = datasetUrls.foreach(element => (0, element)).asInstanceOf[Map[AnyVal, Any]]
    //    val srdd = new sciNd4jRDD[INDArray](sc, datasetMapping, "TotCldLiqH2O_A")

    //    val collected = srdd.collect
    //    collected.map(p => println(p))
    //    sc.stop()
    assert(true)
  }

}