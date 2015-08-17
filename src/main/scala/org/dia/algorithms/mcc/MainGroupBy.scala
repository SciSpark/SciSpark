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

import org.dia.TRMMUtils.Parsers
import org.dia.core.{SciSparkContext, sRDD, sciTensor}
import org.slf4j.Logger

import scala.collection.mutable
import scala.io.Source
import scala.language.implicitConversions


object MainGroupBy {
  val LOG: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    /**
     * Input arguements to the program :
     * args(0) - the path to the source file
     * args(1) - the spark master url. Example : spark://HOST_NAME:7077
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

    /**
     * Parse the date from each URL.
     * Compute the maps from Date to element index.
     * The DateIndexTable holds the mappings.
     *
     */
    val DateIndexTable = new mutable.HashMap[String, Int]()
    val URLs = Source.fromFile(inputFile).mkString.split("\n").toList

    val orderedDateList = URLs.map(p => {
      val source = p.split("/").last.replaceAllLiterally(".", "/")
      Parsers.ParseDateFromString(source)
    }).sorted

    for (i <- orderedDateList.indices) {
      val dateFormat = new SimpleDateFormat("YYYY-MM-dd")
      val dateString = dateFormat.format(orderedDateList(i))
      DateIndexTable += ((dateString, i))
    }

    /**
     * Initialize the spark context to point to the master URL
     *
     */
    val sc = new SciSparkContext(masterURL, "DGTG : Distributed MCC Search")

    /**
     * Ingest the input file and construct the sRDD.
     * For MCC the sources are used to map date-indexes.
     * The metadata variable "FRAME" corresponds to an index.
     * The indices themselves are numbered with respect to
     * date-sorted order.
     */
    val sRDD = sc.randomMatrices(inputFile, List(variable), partCount)
    val labeled = sRDD.map(p => {
      val source = p.metaData("SOURCE").split("/").last.replaceAllLiterally(".", "/")
      val date = new SimpleDateFormat("YYYY-MM-dd").format(Parsers.ParseDateFromString(source))
      val FrameID = DateIndexTable(date)
      p.insertDictionary(("FRAME", FrameID.toString))
      p
    })

    /**
     * The MCC algorithim : Mining for graph vertices and edges
     */
    val filtered = labeled.map(p => p(variable) <= 241.0)
    val reshaped = filtered.map(p => p(variable).reduceResolution(50))
    val complete = filtered.flatMap(p => {
      List((p.metaData("FRAME").toInt, p), (p.metaData("FRAME").toInt + 1, p))
    }).groupBy(_._1)
      .map(p => p._2.map(e => e._2).toList)
      .filter(p => p.size > 1)
      .map(p => p.sortBy(_.metaData("FRAME").toInt))
      .map(p => (p(0), p(1)))

    val componentFrameRDD = complete.flatMap(p => {
      val compUnfiltered1 = mccOps.findCloudComponents(p._1)
      println("THE SIZE OF COMPONENT 1 : " + p._1.metaData("FRAME") + compUnfiltered1.size)

      val compUnfiltered2 = mccOps.findCloudComponents(p._2)
      println("THE SIZE OF COMPONENT 2 : " + p._2.metaData("FRAME") + " " + compUnfiltered2.size)
      val components1 = compUnfiltered1.filter(checkCriteria)
      val components2 = compUnfiltered2.filter(checkCriteria)
      val componentPairs = for (x <- components1; y <- components2) yield (x, y)
      val overlapped = componentPairs.filter(p => !(p._1.tensor * p._2.tensor).isZero)
      overlapped.map(p => ((p._1.metaData("FRAME"), p._1.metaData("COMPONENT")), (p._2.metaData("FRAME"), p._2.metaData("COMPONENT"))))
    })

    val collectedEdges = componentFrameRDD.collect()
    val vertex = collectedEdges.flatMap(p => List(p._1, p._2)).toSet

    println(vertex.toList.sortBy(p => p._1))
    println(collectedEdges.toList.sorted)
    println(vertex.size)
    println(collectedEdges.length)
    println(complete.toDebugString)
  }

  def checkCriteria(p: sciTensor): Boolean = {
    val hash = p.metaData
    val area = hash("AREA").toDouble
    val tempDiff = hash("DIFFERENCE").toDouble
    (area >= 40.0) || (area < 40.0) && (tempDiff > 10.0)
  }

  def getVertexArray(collection: sRDD[sciTensor]): mutable.HashMap[(String, String), Long] = {
    val id = collection.map(p => (p.metaData("FRAME"), p.metaData("COMPONENT"))).collect().toList
    val size = id.length
    val range = 0 to (size - 1)
    val hash = new mutable.HashMap[(String, String), Long]
    range.map(p => hash += ((id(p), p)))
    hash
  }
}


