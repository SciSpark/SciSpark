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

import java.io.{File, PrintWriter}

import scala.collection.mutable

import org.apache.spark.rdd.RDD

import org.dia.core.{SciSparkContext, SciTensor}

/**
 * Runs Grab em' Tag em' Graph em'
 * Data is taken from local file system or HDFS through
 * Spark's experimental "sc.binaryFiles".
 */
class GTGRunner(val masterURL: String,
                val paths: String,
                val partitions: Int) {

  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * To create a map of Nodes from the edges found.
   *
   * @param edges sequence of MCCEdge objects
   * @param lat the lattitude dimension array
   * @param lon the longitude dimension array
   * @return
   */
  def createNodeMapFromEdgeList(edges: Seq[MCCEdge],
                                lat: Array[Double],
                                lon: Array[Double]): mutable.HashMap[String, MCCNode] = {

    val MCCNodes = edges.flatMap(edge => List(edge.srcNode, edge.destNode)).distinct
    val MCCNodeKeyValuesSet = MCCNodes.map(node => {
      val key = s"${node.frameNum},${node.cloudElemNum}"
      node.updateLatLon(lat, lon)
      (key, node)
    })
    mutable.HashMap[String, MCCNode](MCCNodeKeyValuesSet: _*)
  }

  /**
   * Records the frame number in all SciTensors stored in the RDD
   * Preconditon : The files read are are of the form merg_XX_4km-pixel.nc
   *
   * @param sRDD input RDD of SciTensors
   * @return RDD of SciTensors with Frame number recorded in metadaa table
   */
  def recordFrameNumber(sRDD: RDD[SciTensor]): RDD[SciTensor] = {
    sRDD.map(p => {
      val source = p.metaData("SOURCE").split("/").last.split("_")(1)
      val FrameID = source.toInt
      p.insertDictionary(("FRAME", FrameID.toString))
      p.insertVar(p.varInUse, p()(0))
      p
    })
  }

  /**
   * For each array N* where N is the frame number and N* is the array
   * output the following pairs (N, N*), (N + 1, N*).
   *
   * After flat-mapping the pairs and applying additional pre-processing
   * we have pairs (X, Y) where X is a label and Y a tensor.
   *
   * After reducing by key and reordering pairs we obtain pairs
   * (N*, (N+1)*) which achieves the consecutive pairwise grouping
   * of frames.
   *
   * Precondition : Each SciTensor has a FRAME key recorded in its metadata table
   *
   * @param sRDD the input RDD of SciTensors
   * @return
   */
  def pairConsecutiveFrames(sRDD: RDD[SciTensor]): RDD[(SciTensor, SciTensor)] = {
    sRDD.sortBy(p => p.metaData("FRAME").toInt)
      .zipWithIndex()
      .flatMap({ case (sciT, indx) => List((indx, List(sciT)), (indx + 1, List(sciT))) })
      .reduceByKey(_ ++ _)
      .filter({ case (_, sciTs) => sciTs.size == 2 })
      .map({ case (_, sciTs) => sciTs.sortBy(p => p.metaData("FRAME").toInt) })
      .map(sciTs => (sciTs(0), sciTs(1)))
  }

  /**
   * For each consecutive frame pair, find it's components.
   * For each component pairing, find if the element-wise
   * component pairing results in a zero matrix.
   * If not output a new edge pairing of the form ((Frame, Component), (Frame, Component))
   *
   * @param sRDD the input RDD of SciTensors
   * @return
   */
  def findEdges(sRDD: RDD[(SciTensor, SciTensor)],
                maxAreaOverlapThreshold: Double,
                minAreaOverlapThreshold: Double,
                convectiveFraction: Double,
                minArea: Int,
                nodeMinArea: Int): RDD[MCCEdge] = {

    sRDD.flatMap({
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

        val nodeMap = new mutable.HashMap[String, MCCNode]()
        val MCCEdgeMap = new mutable.HashMap[String, MCCEdge]()

        def updateComponent(label: Double, frame: String, value: Double, row: Int, col: Int): Unit = {
          if (label != 0.0) {
            val node = nodeMap.getOrElse(frame + ":" + label, new MCCNode(frame, label))
            node.update(value, row, col)
            nodeMap.update(frame + ":" + label, node)
          }
        }

        for (row <- 0 until product.rows) {
          for (col <- 0 until product.cols) {
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
              val edge = MCCEdgeMap.getOrElse(edgeKey, new MCCEdge(node1, node2))
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
  }

  /**
   * Collect the edges of the form ((String, Double), (String, Double))
   * From the edges collect all used vertices.
   * Repeated vertices are eliminated due to the set conversion.
   * @param MCCEdgeList Collection of MCCEdges
   */
  def processEdges(MCCEdgeList: Iterable[MCCEdge],
                   MCCNodeMap: mutable.HashMap[String, MCCNode]): Unit = {
    logger.info("NUM VERTICES : " + MCCNodeMap.size + "\n")
    logger.info("NUM EDGES : " + MCCEdgeList.size + "\n")

    val pw = new PrintWriter("MCCNodesLines.json")
    MCCNodeMap.foreach { case (key, value) =>
      pw.write(value.toString())
      pw.write("\n")
    }
    pw.close()

    val fw = new PrintWriter("MCCEdges.txt")
    fw.write(MCCEdgeList.toString())
    fw.close()
  }

  def run(): Unit = {
    val maxAreaOverlapThreshold = 0.65
    val minAreaOverlapThreshold = 0.50
    val minArea = 625
    val nodeMinArea = 150
    val convectiveFraction: Double = 0.9

    val outerTemp = 241.0
    val innerTemp = 233.0

    logger.info("Starting MCC")
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
    val sRDD = sc.NetcdfDFSFiles(paths, List("ch4", "longitude", "latitude"), partitions)

    /**
     * Collect lat and lon arrays
     */
    val sampleDataset = sRDD.take(1)(0)
    val lon = sampleDataset("longitude").data
    val lat = sampleDataset("latitude").data

    /**
     * Record the frame Number in each SciTensor
     */
    val labeled = recordFrameNumber(sRDD)


    /**
     * Filter for temperature values under 241.0
     */
    val filtered = labeled.map(p => p("ch4") <= 241.0)


    /**
     * Pair consecutive frames
     */
    val consecFrames = pairConsecutiveFrames(filtered)

    /**
     * Core MCC
     */
    val edgeListRDD = findEdges(consecFrames,
      maxAreaOverlapThreshold,
      minAreaOverlapThreshold,
      convectiveFraction,
      minArea,
      nodeMinArea)

    /**
     * Collect the edgeList and construct NodeMap
     */
    val MCCEdgeList = edgeListRDD.collect()
    val MCCNodeMap = createNodeMapFromEdgeList(MCCEdgeList, lat, lon)

    /**
     * Process the edge list. Collect and output edges and vertices
     */
    processEdges(MCCEdgeList, MCCNodeMap)

    /**
     * Output RDD DAG to logger
     */
    logger.info(edgeListRDD.toDebugString + "\n")
  }

}

