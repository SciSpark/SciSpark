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

import scala.collection.mutable
import scala.io.Source
import scala.language.implicitConversions

import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._

import org.dia.Parsers
import org.dia.core.SciSparkContext
import org.dia.utils.{FileUtils, JsonUtils}

/**
 * Implements MCC with GroupBy + Cartesian product.
 */
object MainGroupByCartesian {

  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    /**
     * Input arguments to the program :
     * args(0) - the path to the source file
     * args(1) - the spark master URL. Example : spark://HOST_NAME:7077
     * args(2) - the number of desired partitions. Default : 2
     * args(3) - square matrix dimension. Default : 20
     * args(4) - variable name
     *
     */
    val inputFile = if (args.isEmpty) "TestLinks" else args(0)
    val masterURL = if (args.length <= 1) "local[12]" else args(1)
    val partCount = if (args.length <= 2) 2 else args(2).toInt
    val dimension = if (args.length <= 3) (20, 20) else (args(3).toInt, args(3).toInt)
    val variable = if (args.length <= 4) "TotCldLiqH2O_A" else args(4)
    val jsonOut = if (args.length <= 5) "" else args(5)

    /**
     * Parse the date from each URI.
     * Compute the maps from date to element index.
     * The DateIndexTable holds the mappings.
     */
    val URIs = Source.fromFile(inputFile).mkString.split("\n").toList
    val DateIndexTable = new mutable.HashMap[String, Int]()

    val orderedDateList = URIs.map(p => {
      val source = p.split("/").last.replaceAllLiterally(".", "/")
      Parsers.parseDateFromString(source)
    }).sorted

    orderedDateList.indices.foreach { i =>
      val dateFormat = new SimpleDateFormat("YYYY-MM-dd")
      val dateString = dateFormat.format(orderedDateList(i))
      DateIndexTable += ((dateString, i))
    }

    /**
     * Initialize the spark context to point to the master URL
     */
    val sc = new SciSparkContext(masterURL, "DGTG : Distributed MCC Search")

    /**
     * Ingest the input file and construct the SRDD.
     * For MCC the sources are used to map date-indexes.
     * The metadata variable "FRAME" corresponds to an index.
     * The indices themselves are numbered with respect to
     * date-sorted order.
     */
    val sRDD = sc.randomMatrices(inputFile, List(variable), dimension, partCount)
    val labeled = sRDD.map(p => {
      val source = p.metaData("SOURCE").split("/").last.replaceAllLiterally(".", "/")
      val date = new SimpleDateFormat("YYYY-MM-dd").format(Parsers.parseDateFromString(source))
      val FrameID = DateIndexTable(date)
      p.insertDictionary(("FRAME", FrameID.toString))
      p
    })

    /**
     * The MCC algorithm : Mining for graph vertices and edges
     */
    val filtered = labeled.map(p => p(variable) <= 241.0)

    val consecFrames = filtered.flatMap(p => {
      List((p.metaData("FRAME").toInt, p), (p.metaData("FRAME").toInt + 1, p))
    }).groupBy(_._1)
      .map(_._2.map(e => e._2).toList)
      .filter(_.size > 1)
      .map(_.sortBy(_.metaData("FRAME").toInt))
      .map(p => (p(0), p(1)))

    /**
     * Core MCC using Cartesian product approach.
     */
    val componentFrameRDD = consecFrames.flatMap(p => {
      val compsUnfiltered1 = MCCOps.findCloudComponents(p._1)
      val compsUnfiltered2 = MCCOps.findCloudComponents(p._2)
      val comps1 = compsUnfiltered1.filter(MCCOps.checkCriteria)
      val comps2 = compsUnfiltered2.filter(MCCOps.checkCriteria)
      val compPairs = for (x <- comps1; y <- comps2) yield (x, y)
      val overlaps = compPairs.filter({ case (t1, t2) => !(t1.tensor * t2.tensor).isZeroShortcut })
      overlaps.map({
        case (t1, t2) =>
        ((t1.metaData("FRAME"), t1.metaData("COMPONENT")), (t2.metaData("FRAME"), t2.metaData("COMPONENT")))
      })
    })

    /**
     * Collect the edges.
     * From the edge pairs collect all used vertices.
     * Repeated vertices are eliminated due to the set conversion.
     */
    val collectedEdges = componentFrameRDD.collect()
    val collectedVertices = collectedEdges.flatMap({ case (n1, n2) => List(n1, n2) }).toSet

    println(collectedVertices.size)
    println(collectedEdges.length)
    println(consecFrames.toDebugString)

    /**
     * Write result graph out to JSON.
     */
    if (!jsonOut.isEmpty) {
      val (jnodes, jedges) = JsonUtils.generateJson(collectedVertices, collectedEdges, DateIndexTable)
      val json = ("nodes" -> jnodes) ~ ("edges" -> jedges)
      FileUtils.writeToFile(jsonOut, pretty(render(json)))
    }
  }
}
