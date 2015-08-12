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
package org.dia.algorithms.mcc

import org.apache.spark.rdd.RDD
import org.dia.Constants._
import org.dia.core.{SciSparkContext, sRDD, sciTensor}

import scala.collection.mutable
import scala.language.implicitConversions

/**
  */
object Mainp {

  /**
   * NetCDF variables to use
   * TODO:: Make the netcdf variables global - however this may need broadcasting
   */
  val rowDim = 180
  val columnDim = 360
  val TextFile = "TestLinks"


  def main(args: Array[String]): Unit = {
    var master = ""
    val testFile = if (args.isEmpty) "TestLinks2" else args(0)
    if (args.isEmpty || args.length <= 1) master = "local[24]" else master = args(1)

    val sc = new SciSparkContext(master, "test")
    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)

    val variable = if (args.isEmpty || args.length <= 2) "TotCldLiqH2O_A" else args(2)
    val RDDmetatuple = sc.NetcdfFile(testFile, List(variable), 1)

    val sRDD = RDDmetatuple._1
    val dateMap = RDDmetatuple._2

    println(dateMap)

    //val preCollected = sRDD.map(p => p(variable).reduceResolution(5))

    val filtered = sRDD.map(p => p(variable) <= 241.0)

    val componentFrameRDD = filtered.flatMap(p => mccOps.findCloudComponents(p))

    val criteriaRDD = componentFrameRDD.filter(p => {
      val hash = p.metaData
      val area = hash("AREA").toDouble
      val tempDiff = hash("DIFFERENCE").toDouble
      (area >= 40.0) || (area < 40.0) && (tempDiff > 10.0)
    })

    val vertexSet = getVertexArray(criteriaRDD)
    println(vertexSet)
    val dateMappedRDDs = dateMap.map(p => (p._1, criteriaRDD.filter(_.metaData("FRAME") == p._1.toString))).toList.sortBy(p => p._1)
    println(dateMappedRDDs)
    val hash = new mutable.HashMap() ++ dateMappedRDDs

    var edgeRDD: RDD[(Long, Long)] = null
    for (index <- 0 to dateMappedRDDs.size - 2) {
      val currentTimeRDD = hash(index)
      val nextTimeRDD = hash(index + 1)
      val cartesianPair = currentTimeRDD.cartesian(nextTimeRDD)
      val findEdges = cartesianPair.filter(p => !(p._1.tensor * p._2.tensor).isZero)
      val edgePair = findEdges.map(p => (vertexSet(p._1.metaData("FRAME"), p._1.metaData("COMPONENT")), vertexSet(p._2.metaData("FRAME"), p._2.metaData("COMPONENT"))))
      if (edgeRDD == null) {
        edgeRDD = edgePair
      } else {
        edgeRDD = edgeRDD ++ edgePair
      }
    }

    val collectedEdges = edgeRDD.collect()
    //println(edgeRDD.toDebugString)
    vertexSet.foreach(p => println(p))
    collectedEdges.foreach(p => println(p))
    println(collectedEdges.length)
    println(vertexSet.toList.size)
  }

  def getVertexArray(collection: sRDD[sciTensor]): mutable.HashMap[(String, String), Long] = {
    val id = collection.map(p => (p.metaData("FRAME") , p.metaData("COMPONENT"))).collect().toList
    val size = id.length
    val range = 0 to (size - 1)
    val hash = new mutable.HashMap[(String, String), Long]
    range.map(p => hash += ((id(p), p)))
    hash
  }
}


