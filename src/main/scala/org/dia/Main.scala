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
package org.dia

import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
import org.dia.Constants._
import org.dia.TRMMUtils.Parsers
import org.dia.core.{EdgeAccumulator, sRDD, SciSparkContext, sciTensor}
import org.dia.sLib.mccOps

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.io.Source
import org.dia.tensors.Nd4jTensor
import org.nd4j.api.Implicits._
import org.nd4j.linalg.factory.Nd4j

import scala.language.implicitConversions

/**
  */
object Main {

  /**
   * NetCDF variables to use
   * TODO:: Make the netcdf variables global - however this may need broadcasting
   */
  val rowDim = 180
  val columnDim = 360
  val TextFile = "TestLinks"


  def main(args: Array[String]): Unit = {
    var master = "";
    var testFile = if (args.isEmpty) "TestLinks" else args(0)
    if(args.isEmpty || args.length <= 1) master = "local[4]" else master = args(1)

    val sc = new SciSparkContext(master, "test")

    sc.setLocalProperty(ARRAY_LIB, ND4J_LIB)

    val variable = if(args.isEmpty || args.length <= 2) "TotCldLiqH2O_A" else args(2)

    val sRDD = sc.NetcdfFile(testFile, List(variable))

    val preCollected = sRDD.map(p => p(variable).reduceResolution(5))

    val filtered = preCollected.map(p => p(variable) <= 0.0009)

    val componentFrameRDD = filtered.flatMap(p => mccOps.findCloudElements(p))

    val criteriaRDD = componentFrameRDD.filter(p => {
      val hash = p.metaData
      val area = hash("AREA").toDouble
      val tempDiff = hash("DIFFERENCE").toDouble
      (area >= 40.0) || (area < 40.0) && (tempDiff > 10.0)
    })

    criteriaRDD.checkpoint
//    val sortedTimeFrames = sRDD.map(p => Parsers.ParseDateFromString(p.metaData("FRAME"))).sortBy(p => p)
//    val sortedDates = sortedTimeFrames.collect
    val dates = Source.fromFile(args(0)).mkString.split("\n").toList.map(p => p.replaceAllLiterally(".", "\\")).map(p => Parsers.ParseDateFromString(p))

    val vertexSet = getVertexArray(criteriaRDD)

    val dateMappedRDDs = dates.map(p => (p, criteriaRDD.filter(_.metaData("FRAME") == p.toString)))
    var edgeRDD : RDD[(Long, Long)] = null
    //var preEdgeAccumulator: Accumulator[List[(Long, Long)]] = sc.sparkContext.accumulator(List((0L, 0L)), "EdgeAccumulation")(EdgeAccumulator)

    for (index <- 0 to dateMappedRDDs.size - 2) {
      println(index)
      val currentTimeRDD = dateMappedRDDs(index)._2
      val nextTimeRDD = dateMappedRDDs(index + 1)._2
      val cartesianPair = currentTimeRDD.cartesian(nextTimeRDD)
//      val findEdges = cartesianPair.filter(p => (p._1.tensor * p._2.tensor).isZero == false)
      val findEdges = cartesianPair
      val edgePair = findEdges.map(p => (vertexSet(p._1.metaData), vertexSet(p._2.metaData)))
      //edgePair.collect.toList.map(p => println(p + "\n"))

      if(edgeRDD == null) {
        edgeRDD = edgePair
      } else {
        edgeRDD = edgeRDD ++ edgePair
      }
    }

    //val Sliced = filtered.map(p => p(variable)(4 -> 9, 2 -> 5))

    //val collected: Array[sciTensor] = criteriaRDD.collect

//    collected.map(p => {
//      println(p)
//    })
    vertexSet.map(p => println(p))
    edgeRDD.collect.map(p => println(p))
    dates.map(p => println(p))
  }

  def getVertexArray(collection: sRDD[sciTensor]): HashMap[HashMap[String, String], Long] = {
    val id = collection.map(p => p.metaData).collect.toList
    val size = id.length
    val range = 0L to (size - 1)
    val hash = new mutable.HashMap[mutable.HashMap[String, String], Long]
    range.map(p => hash += ((id(p.toInt), p)))
    hash
  }
}


