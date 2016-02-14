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

import java.io.{ File, PrintWriter }
import java.text.SimpleDateFormat
import org.dia.Parsers
import org.dia.core.{ SciSparkContext, SRDD, SciTensor }
import org.slf4j.Logger
import scala.collection.mutable
import scala.io.Source
import scala.language.implicitConversions

/**
 * Implements MCC with GroupBy + In-place iteration.
 */
object MainGroupBy {

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
     * Core MCC using in-place iteration.
     * For each consecutive frame pair, find it's components.
     * For each component pairing, find if the element-wise
     * component pairing results in a zero matrix.
     * If not output a new edge pairing in the form ((Frame, Component), (Frame, Component))
     */
    val componentFrameRDD = consecFrames.flatMap(p => {
      val components1 = MCCOps.labelConnectedComponents(p._1.tensor)
      val components2 = MCCOps.labelConnectedComponents(p._2.tensor)
      val product = components1._1 * components2._1

      var ArrList = mutable.MutableList[(Double, Double)]()
      var hashComps = new mutable.HashMap[String, (Double, Double, Double)]

      for (row <- 0 to product.rows - 1) {
        for (col <- 0 to product.cols - 1) {

          /** Add the new edge */
          if (product(row, col) != 0.0) {
            val value1 = components1._1(row, col)
            val value2 = components2._1(row, col)
            ArrList += ((value1, value2))
          }

          /** Update basic statistics about components in sciTensor1 */
          if (components1._1(row, col) != 0.0) {
            var area1 = 0.0
            var max1 = Double.MinValue
            var min1 = Double.MaxValue
            val compMetrics = hashComps.get(p._1.metaData("FRAME") + ":" + components1._1(row, col))
            if (compMetrics != null && compMetrics.isDefined) {
              area1 = compMetrics.get._1
              max1 = compMetrics.get._2
              min1 = compMetrics.get._3
              if (p._1.tensor(row, col) < min1)
                min1 = p._1.tensor(row, col)
              if (p._1.tensor(row, col) > max1)
                max1 = p._1.tensor(row, col)
            } else {
              min1 = p._1.tensor(row, col)
              max1 = p._1.tensor(row, col)
            }
            area1 += 1
            hashComps += ((p._1.metaData("FRAME") + ":" + components1._1(row, col), (area1, max1, min1)))
          }

          /** Update basic statistics about components in sciTensor2 */
          if (components2._1(row, col) != 0.0) {
            var area2 = 0.0
            var max2 = Double.MinValue
            var min2 = Double.MaxValue
            val compMetrics = hashComps.get(p._2.metaData("FRAME") + ":" + components2._1(row, col))
            if (compMetrics != null && compMetrics.isDefined) {
              area2 = compMetrics.get._1
              max2 = compMetrics.get._2
              min2 = compMetrics.get._3
              if (p._2.tensor(row, col) < min2)
                min2 = p._2.tensor(row, col)
              if (p._2.tensor(row, col) > max2)
                max2 = p._2.tensor(row, col)
            } else {
              min2 = p._2.tensor(row, col)
              max2 = p._2.tensor(row, col)
            }
            area2 += 1
            hashComps += ((p._2.metaData("FRAME") + ":" + components2._1(row, col), (area2, max2, min2)))
          }

        }
      }

      val edgesSet = ArrList.toSet
      val edges = edgesSet.map(x => ((p._1.metaData("FRAME"), x._1), (p._2.metaData("FRAME"), x._2)))

      /**
       * Filter out edges whose nodes are actually
       * clouds according to some criterion.
       */
      val overlaps = edges.filter({
        case (n1, n2) => {
          val frameId1 = n1._1
          val compId1 = n1._2
          val (area1, max1, min1) = hashComps(frameId1 + ":" + compId1)
          val isCloud1 = ((area1 >= 2400.0) || ((area1 < 2400.0) && ((min1/max1) > 0.9)))

          val frameId2 = n2._1
          val compId2 = n2._2
          val (area2, max2, min2) = hashComps(frameId2 + ":" + compId2)
          val isCloud2 = ((area2 >= 2400.0) || ((area2 < 2400.0) && ((min2/max2) > 0.9)))
          isCloud1 && isCloud2
        }
      })
      overlaps
    })

    /**
     * Collect the edges.
     * From the edge pairs collect all used vertices.
     * Repeated vertices are eliminated due to the set conversion.
     */
    val collectedEdges = componentFrameRDD.collect()
    val collectedVertices = collectedEdges.flatMap({ case (n1, n2) => List(n1, n2) }).toSet

    val out = new PrintWriter(new File("VertexAndEdgeList.txt"))
    out.write(collectedVertices.toList.sortBy(_._1) + "\n")
    out.write(collectedEdges.toList.sorted + "\n")
    out.close()
    println("NUM VERTICES : " + collectedVertices.size + "\n")
    println("NUM EDGES : " + collectedEdges.length + "\n")
    println(consecFrames.toDebugString + "\n")
  }

}


