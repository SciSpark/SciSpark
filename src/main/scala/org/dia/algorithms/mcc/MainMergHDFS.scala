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
import org.dia.core.{ SciSparkContext, SciTensor }
import org.slf4j.Logger
import scala.collection.mutable
import scala.io.Source
import scala.language.implicitConversions

/**
 * Implements MCC with GroupBy + in-place iteration.
 * Data is taken through SciSparkContext's mergDFSFile.
 */
object MainMergHDFS {

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
    val inputFile = if (args.isEmpty) "TRMM_L3_Links.txt" else args(0)
    val masterURL = if (args.length <= 1) "local[12]" else args(1)
    val partCount = if (args.length <= 2) 2 else args(2).toInt
    val dimension = if (args.length <= 3) (20, 20) else (args(3).toInt, args(3).toInt)
    val variable = if (args.length <= 4) "TMP" else args(4)
    val hdfspath = if (args.length <= 5) "resources/merg" else args(5)

    /**
     * Parse the date from each URL.
     * Compute the maps from Date to element index.
     * The DateIndexTable holds the mappings.
     *
     */
    val dateIndexTable = new mutable.HashMap[String, Int]()
    val uris = Source.fromFile(inputFile).mkString.split("\n").toList

    val orderedDateList = uris.map(p => {
      p.split("/").last.split("_")(1) //replaceAllLiterally(".", "/")
    }).sorted

    for (i <- orderedDateList.indices) {
      dateIndexTable += ((orderedDateList(i), i))
    }

    /**
     * Initialize the spark context to point to the master URL
     *
     */
    val sc = new SciSparkContext(masterURL, "DGTG : Distributed MCC Search")

    /**
     * Ingest the input file and construct the SRDD.
     * For MCC the sources are used to map date-indexes.
     * The metadata variable "FRAME" corresponds to an index.
     * The indices themselves are numbered with respect to
     * date-sorted order.
     *
     * Note if no HDFS path is given, then randomly generated matrices are used
     *
     */
    val sRDD = if (hdfspath != null) {
      sc.mergDFSFile(hdfspath, List(variable), partCount)
    } else {
      sc.randomMatrices(inputFile, List(variable), dimension, partCount)
    }

    val labeled = sRDD.map(p => {
      val source = p.metaData("SOURCE").split("/").last.split("_")(1)
      val FrameID = dateIndexTable(source)
      p.insertDictionary(("FRAME", FrameID.toString))
      p
    })

    /**
     * The MCC algorithm : Mining for graph vertices and edges
     *
     * For each array N* where N is the frame number and N* is the array,
     * output the following pairs (N, N*), (N + 1, N*).
     *
     * After flat-mapping the pairs and applying additional pre-processing
     * we have pairs (X, Y) where X is a label and Y a tensor.
     *
     * After grouping by X and reordering we obtain pairs
     * (N*, (N+1)*) which achieves the consecutive pairwise grouping
     * of frames.
     */
    val filtered = labeled.map(p => p(variable) <= 241.0)
    val consecFrames = filtered.flatMap(p => {
      List((p.metaData("FRAME").toInt, p), (p.metaData("FRAME").toInt + 1, p))
    }).groupBy(_._1)
      .filter(p => p._2.size > 1)
      .map(p => {
        val list = p._2.map(e => e._2).toList
        val sortedList = list.sortBy(_.metaData("FRAME").toInt)
        (sortedList(0), sortedList(1))
      })

    /**
     * Core MCC
     * For each consecutive frame pair, find it's components.
     * For each component pairing, find if the element-wise
     * component pairing results in a zero matrix.
     * If not output a new edge pairing in the form ((frameId, componentId), (frameId, componentId))
     */
    val componentFrameRDD = consecFrames.flatMap({
      case (t1, t2) => {
        /**
         * First label the connected components in each pair.
         * The following example illustrates labeling.
         *
         * [0,1,2,0]       [0,1,1,0]
         * [1,2,0,0]   ->  [1,1,0,0]
         * [0,0,0,1]       [0,0,0,2]
         *
         * Note that a tuple of (Matrix, MaxLabel) is returned
         * to denote the labeled elements and the highest label.
         * This way only one traverse is necessary instead of a 2nd traverse
         * to find the highest label.
         */
        val (components1, _) = MCCOps.labelConnectedComponents(t1.tensor)
        val (components2, _) = MCCOps.labelConnectedComponents(t2.tensor)
        /**
         * The labeled components are element-wise multiplied
         * to find overlapping regions. Non-overlapping regions
         * result in a 0.
         *
         * [0,1,1,0]       [0,1,1,0]     [0,1,1,0]
         * [1,1,0,0]   X   [2,0,0,0]  =  [2,0,0,0]
         * [0,0,0,2]       [0,0,0,3]     [0,0,0,6]
         *
         */
        val product = components1 * components2
        /**
         * The overlappedPairsList keeps track of all points that
         * overlap between the labeled arrays. Note that the overlappedPairsList
         * will have several duplicate pairs if there are large regions of overlap.
         *
         * This is achieved by iterating through the product array and noting
         * all points that are not 0.
         */
        var overlappedPairsList = mutable.MutableList[(Double, Double)]()
        /**
         * The areaMinMaxTable keeps track of the area, minimum value, and maximum value
         * of all components in both frames. For this reason the hash key has the following form :
         * 'F : C' where F = Frame Number and C = Component Number
         */
        var areaMinMaxTable = new mutable.HashMap[String, (Double, Double, Double)]

        def updateComponent(label: Double, frame: String, value: Double): Unit = {
          if (label != 0.0) {
            var area = 0.0
            var max = Double.MinValue
            var min = Double.MaxValue
            val currentProperties = areaMinMaxTable.get(frame + ":" + label)
            if (currentProperties != null && currentProperties.isDefined) {
              area = currentProperties.get._1
              max = currentProperties.get._2
              min = currentProperties.get._3
              if (value < min) min = value
              if (value > max) max = value
            } else {
              min = value
              max = value
            }
            area += 1
            areaMinMaxTable += ((frame + ":" + label, (area, max, min)))
          }
        }

        for (row <- 0 to product.rows - 1) {
          for (col <- 0 to product.cols - 1) {
            /** Find non-zero points in product array */
            if (product(row, col) != 0.0) {
              /** save components ids */
              val value1 = components1(row, col)
              val value2 = components2(row, col)
              overlappedPairsList += ((value1, value2))
            }
            updateComponent(components1(row, col), t1.metaData("FRAME"), t1.tensor(row, col))
            updateComponent(components2(row, col), t2.metaData("FRAME"), t2.tensor(row, col))
          }
        }
        /**
         * Once the overlapped pairs have been computed, eliminate all duplicates
         * by converting the collection to a set. The component edges are then
         * mapped to the respective frames, so the global space of edges (outside of this task)
         * consists of unique tuples.
         */
        val edgesSet = overlappedPairsList.toSet
        val edges = edgesSet.map({ case (c1, c2) => ((t1.metaData("FRAME"), c1), (t2.metaData("FRAME"), c2)) })

        val filtered = edges.filter({
          case ((frameId1, compId1), (frameId2, compId2)) => {
            val (area1, max1, min1) = areaMinMaxTable(frameId1 + ":" + compId1)
            val isCloud1 = ((area1 >= 2400.0) || ((area1 < 2400.0) && ((min1/max1) > 0.9)))
            val (area2, max2, min2) = areaMinMaxTable(frameId2 + ":" + compId2)
            val isCloud2 = ((area2 >= 2400.0) || ((area2 < 2400.0) && ((min2/max2) > 0.9)))
            isCloud1 && isCloud2
          }
        })
        filtered
      }
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


