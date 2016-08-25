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

import java.io._

import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import org.dia.core.{SciSparkContext, SciTensor}


object NetcdfDFSMCC {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * To create a map of Nodes from the edges found.
   *
   * @param edgesRDD
   * @param lat
   * @param lon
   * @return
   */
  def createMapFromEdgeList(edgesRDD: Iterable[MCCEdge],
                            lat: Array[Double], lon: Array[Double]): mutable.HashMap[String, Any] = {
    val MCCNodeMap = new mutable.HashMap[String, Any]()
    edgesRDD.foreach { edge => {
      val srcKey = s"${edge.srcNode.frameNum},${edge.srcNode.cloudElemNum}"
      val destKey = s"${edge.destNode.frameNum},${edge.destNode.cloudElemNum}"

      if (!MCCNodeMap.contains(srcKey)) {
        edge.srcNode.updateLatLon(lat, lon)
        MCCNodeMap += ((srcKey, edge.srcNode))
      }
      if (!MCCNodeMap.contains(destKey)) {
        edge.destNode.updateLatLon(lat, lon)
        MCCNodeMap += ((destKey, edge.destNode))
      }
    }
    }
    return MCCNodeMap
  }

  /**
   * For each array N* where N is the frame number and N* is the array
   * output the following pairs (N, N*), (N + 1, N*).
   *
   * After flat-mapping the pairs and applying additional pre-processing
   * we have pairs (X, Y) where X is a label and Y a tensor.
   *
   * After grouping by X and reordering pairs we obtain pairs
   * (N*, (N+1)*) which achieves the consecutive pairwise grouping
   * of frames.
   */
  def groupConsecutiveFrames(sRDD: RDD[SciTensor]): RDD[(SciTensor, SciTensor)] = {
    val consecFrames = sRDD.sortBy(p => p.metaData("FRAME").toInt).zipWithIndex()
      .flatMap({ case (sciT, indx) => List((indx, List(sciT)), (indx + 1, List(sciT))) })
      .reduceByKey(_ ++ _)
      .filter({ case (_, sciTs) => sciTs.size == 2 })
      .map({ case (_, sciTs) => sciTs.sortBy(p => p.metaData("FRAME").toInt) })
      .map(sciTs => (sciTs(0), sciTs(1)))
    return consecFrames
  }

  def createLabelledRDD(sRDD: RDD[SciTensor]): RDD[SciTensor] = {
    val labeled = sRDD.map(p => {
      val source = p.metaData("SOURCE").split("/").last.split("_")(1)
      val FrameID = source.toInt
      p.insertDictionary(("FRAME", FrameID.toString))
      p.insertVar(p.varInUse, p()(0))
      p
    })
    return labeled
  }

  /**
   * Find edges matching certain filtering criteria from the consecutive
   * frames found in the NetCDF files.
   *
   * @param consecFrames
   * @param maxAreaOverlapThreshold
   * @param minAreaOverlapThreshold
   * @param minArea
   * @return
   */
  def findEdges(consecFrames: RDD[(SciTensor, SciTensor)], maxAreaOverlapThreshold: Double,
                minAreaOverlapThreshold: Double, minArea: Double,
                nodeMinArea: Int, convectiveFraction: Double): RDD[MCCEdge] = {
    val componentFrameRDD = consecFrames.flatMap({
      case (t1, t2) =>

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

        var nodeMap = new mutable.HashMap[String, MCCNode]()
        val MCCEdgeMap = new mutable.HashMap[String, MCCEdge]()

        def updateComponent(label: Double, frame: String, value: Double, row: Int, col: Int): Unit = {
          if (label != 0.0) {
            val node = nodeMap.getOrElse(frame + ":" + label, new MCCNode(frame, label))
            node.update(value, row, col)
            nodeMap.update((frame + ":" + label), node)
          }
        }


        for (row <- 0 to product.rows - 1) {
          for (col <- 0 to product.cols - 1) {
            /** Find non-zero points in product array */
            updateComponent(components1(row, col), t1.metaData("FRAME"), t1.tensor(row, col), row, col)
            updateComponent(components2(row, col), t2.metaData("FRAME"), t2.tensor(row, col), row, col)
            if (product(row, col) != 0.0) {

              /** If overlap exists create and edge and update overlapped area */
              val frame1 = t1.metaData("FRAME")
              val label1 = components1(row, col)
              val node1 = nodeMap(frame1 + ":" + label1)

              val frame2 = t2.metaData("FRAME")
              val label2 = components2(row, col)
              val node2 = nodeMap(frame2 + ":" + label2)

              val edgeKey = s"$frame1:$label1,$frame2:$label2"
              val edge = if (MCCEdgeMap.contains(edgeKey)) MCCEdgeMap(edgeKey) else new MCCEdge(node1, node2)
              edge.incrementAreaOverlap()
              MCCEdgeMap.update(edgeKey, edge)
            }
          }
        }

        val filtered = MCCEdgeMap.filter({
          case (k, edge) =>
            val srcNode = edge.srcNode
            val (srcArea, srcMinTemp, srcMaxTemp) = (srcNode.area, srcNode.minTemp, srcNode.maxTemp)
            val isSrcNodeACloud = (srcArea >= nodeMinArea) ||
              (srcArea < nodeMinArea && (srcMinTemp / srcMaxTemp) < convectiveFraction)

            val destNode = edge.destNode
            val (destArea, destMinTemp, destMaxTemp) = (destNode.area, destNode.minTemp, destNode.maxTemp)
            val isDestNodeACloud = (destArea >= nodeMinArea) ||
              (destArea < nodeMinArea && (destMinTemp / destMaxTemp) < convectiveFraction)
            var meetsOverlapCriteria = true
            if (isSrcNodeACloud && isDestNodeACloud) {
              val areaOverlap = edge.areaOverlap
              val srcAreaOverlapRation: Double = areaOverlap.toDouble / srcArea.toDouble
              val destAreaOverlapRation: Double = areaOverlap.toDouble / destArea.toDouble
              val percentAreaOverlap = math.max(srcAreaOverlapRation, destAreaOverlapRation)

              if (percentAreaOverlap >= maxAreaOverlapThreshold) {
                edge.updateWeight(1.0)
              }
              else if (percentAreaOverlap < maxAreaOverlapThreshold &&
                percentAreaOverlap >= minAreaOverlapThreshold) {
                edge.updateWeight(2.0)
              }
              else if (areaOverlap >= minArea) {
                edge.updateWeight(3.0)
              }
              else {
                meetsOverlapCriteria = false
              }
            }
            isSrcNodeACloud && isDestNodeACloud && meetsOverlapCriteria
        })
        filtered.values
    })
    return componentFrameRDD
  }

  def main(args: Array[String]): Unit = {
    /**
     * Input arguments to the program :
     * args(0) - the spark master URL. Example : spark://HOST_NAME:7077
     * args(1) - the number of desired partitions. Default : 2
     * args(2) - square matrix dimension. Default : 20
     * args(3) - variable name
     * args(4) - local path to files
     *
     */
    val masterURL = if (args.isEmpty) "local[2]" else args(0)
    val partCount = if (args.length <= 1) 2 else args(1).toInt
    val dimension = if (args.length <= 2) (20, 20) else (args(2).toInt, args(2).toInt)
    val variable = if (args.length <= 3) "ch4" else args(3)
    val hdfspath = if (args.length <= 4) "resources/merg" else args(4)
    val maxAreaOverlapThreshold = 0.65
    val minAreaOverlapThreshold = 0.50
    val minArea = 625
    val nodeMinArea = 150
    val convectiveFraction: Double = 0.9

    val outerTemp = 241.0
    val innerTemp = 233.0
    println("Starting MCC")

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
     *
     * Note if no HDFS path is given, then randomly generated matrices are used.
     *
     */
    val sRDD = sc.NetcdfDFSFiles(hdfspath, List("ch4", "longitude", "latitude"), partCount)
    val labeled = createLabelledRDD(sRDD)

    val collected = labeled.collect()
    val lon = collected(0).variables("longitude").data
    val lat = collected(0).variables("latitude").data

    val filtered = labeled.map(p => p(variable) <= 241.0)

    val consecFrames = groupConsecutiveFrames(filtered)

    val edgesRDD = findEdges(consecFrames, maxAreaOverlapThreshold,
      minAreaOverlapThreshold, minArea, nodeMinArea, convectiveFraction)

    val MCCEdgeList = edgesRDD.collect().toList
    val MCCNodeMap = createMapFromEdgeList(MCCEdgeList, lat, lon)

    println("NUM OF NODES : " + MCCNodeMap.size)
    println("NUM OF EDGES : " + MCCEdgeList.size)


    val pw = new PrintWriter("MCCNodesLines.json")
    MCCNodeMap.foreach { case (key, value) =>
      pw.write(value.toString)
      pw.write("\n")
    }
    pw.close()

    val fw = new FileWriter("MCCEdges.txt")
    fw.write(MCCEdgeList.toString())
    fw.close()


  }
}

