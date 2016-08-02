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

import java.text.SimpleDateFormat
import org.apache.spark.rdd.RDD
import org.dia.Constants._
import org.dia.Parsers
import org.dia.core.{ SciSparkContext, SRDD, SciTensor }
import scala.collection.mutable
import scala.io.Source
import scala.language.implicitConversions

/**
 * This is doing the "naive port of GTG" (see paper).
 */
object MainPorted {

  /**
   * NetCDF variables to use
   *
   * @todo Make the NetCDF variables global - however this may need broadcasting
   */
  val rowDim = 20
  val colDim = 20
  val textFile = "TestLinks"

  def main(args: Array[String]): Unit = {

    val master = if (args.isEmpty || args.length <= 1) "local[24]" else args(1)
    val testFile = if (args.isEmpty) textFile else args(0)

    val sc = new SciSparkContext(master, "test")
    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)

    val partitionNum = if (args.isEmpty || args.length <= 2) 2 else args(2).toInt
    val variable = if (args.isEmpty || args.length <= 3) "TotCldLiqH2O_A" else args(3)
    val dimension = if (args.isEmpty || args.length <= 4) (rowDim, colDim) else (args(4).toInt, args(4).toInt)
    val sRDD = sc.randomMatrices(testFile, List(variable), dimension, partitionNum)

    val dateMap = new mutable.HashMap[Int, String]()
    val uris = Source.fromFile(testFile).mkString.split("\n").toList

    val orderedDateList = uris.map(p => {
      val source = p.split("/").last.replaceAllLiterally(".", "/")
      val date = Parsers.parseDateFromString(source)
      new SimpleDateFormat("YYYY-MM-DD").format(date)
    }).sorted

    for (i <- orderedDateList.indices) {
      dateMap += ((i, orderedDateList(i)))
    }

    val filtered = sRDD.map(p => p(variable) <= 241.0)
    val componentFrameRDD = filtered.flatMap(MCCOps.findCloudComponents)
    val criteriaRDD = componentFrameRDD.filter(MCCOps.checkCriteria)

    /** Get vertices first */
    val vertices = getVertexArray(criteriaRDD)
    println(vertices)

    /**
     *  Compare to Fig. 6 in the paper.
     */
    val dateMappedRDDs = dateMap.map({ case (id, _) =>
      (id, criteriaRDD.filter(_.metaData("FRAME") == id.toString)) }).toList.sortBy(_._1)
    println(dateMappedRDDs)
    val hash = new mutable.HashMap() ++ dateMappedRDDs

    /**
     *  Get edges next.
     *  This is extremely inefficient, as described in the paper.
     */
    var edgeRDD: RDD[(Long, Long)] = null
    for (index <- 0 to dateMappedRDDs.size - 2) {
      val currentTimeRDD = hash(index)
      val nextTimeRDD = hash(index + 1)
      val cartesianPair = currentTimeRDD.cartesian(nextTimeRDD)
      val findEdges = cartesianPair.filter({ case (t1, t2) => !(t1.tensor * t2.tensor).isZeroShortcut })
      val edgePair = findEdges.map({ case (t1, t2) =>
            val vertex1 = vertices(t1.metaData("FRAME"), t1.metaData("COMPONENT"))
            val vertex2 = vertices(t2.metaData("FRAME"), t2.metaData("COMPONENT"))
            (vertex1, vertex2)
        })
      if (edgeRDD == null) {
        edgeRDD = edgePair
      } else {
        edgeRDD = edgeRDD ++ edgePair
      }
    }

    val edges = edgeRDD.collect()
    vertices.foreach(println)
    edges.foreach(println)
    println(edges.length)
    println(vertices.toList.size)
  }

  /**
   * Gets vertices names out of SRDD of component SciTensor's.
   *
   * @param collection the SRDD of component SciTensor's
   * @return a hash map of entries key = (frameID, componentID) and value = globalID
   */
  def getVertexArray(collection: SRDD[SciTensor]): mutable.HashMap[(String, String), Long] = {
    val id = collection.map(p => (p.metaData("FRAME"), p.metaData("COMPONENT"))).collect().toList
    val size = id.length
    val range = 0 to (size - 1)
    val hash = new mutable.HashMap[(String, String), Long]
    range.map(p => hash += ((id(p), p)))
    hash
  }

}


