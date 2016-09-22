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

import java.io.FileWriter
import java.io.PrintWriter
import java.util

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.dia.core.SciDataset
import org.dia.tensors.AbstractTensor



/**
 * Utilities to compute connected components within tensor.
 */
object MCCOps {

  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Whether two components overlap.
   *
   * @param comps1 first component tensor
   * @param comps2 second component tensor
   * @param compNum1 first component number
   * @param compNum2 second component number
   * @return whether the two components overlap
   * @todo this method only makes sense if the two tensors are component tensors,
   * i.e. the values are the component numbers not normal values. make sure that
   * this method is thus only callable with such tensors, e.g. make it only callable
   * from within checkComponentsOverlap.
   */
  def overlap(comps1: AbstractTensor, comps2: AbstractTensor, compNum1: Int, compNum2: Int): Boolean = {
    /** mask for specific component */
    val maskedComp1 = comps1.map(e => {
      if (e == compNum1) 1.0 else 0.0
    })
    val maskedComp2 = comps1.map(e => {
      if (e == compNum2) 1.0 else 0.0
    })
    /** check overlap */
    !(maskedComp1 * maskedComp2).isZeroShortcut
  }

  /**
   * Method to partition the edges into buckets containing a group of
   * consecutive nodes
   * @param edge MCCEdge
   * @param bucketSize Number of nodes to put in one bucket (size of partition)
   * @param partitionCount Number of partitions required
   * @return A bucket number (int) and edge
   */
  def mapEdgesToBuckets(edge: MCCEdge, bucketSize: Int, partitionCount: Int): (Int, MCCEdge) = {
    val edgePartitionKey: Int = edge.metadata("index").toInt
    for (i <- 1 to partitionCount) {
      if (edgePartitionKey <= bucketSize * i) {
        val bucket: Int = i
        return (bucket, edge)
      }
    }
    /** Place the edge in the last partition, if in none of the above */
    return (partitionCount, edge)
  }

  /**
   * @todo Come up with a way to find out border nodes when frame numbers are of type 2006091100
   * @param partition
   * @param currentIteration
   * @param bucketSize
   * @return
   */
  def processEdgePartition(
      partition: (Int, Iterable[MCCEdge]),
      currentIteration: Int,
      minGraphLength: Int,
      bucketSize: Int): (Int, (Iterable[MCCEdge], mutable.MutableList[mutable.HashSet[String]])) = {

    logger.info(s"Processing partition for key: ${partition._1} at iteration: $currentIteration" +
      s" with edges: ${partition._2}")

    /** The current max-size of the partition, since we recursively merge two paritions
     * we get the partition size by multiplying the # of iteration and individual partition size
     */
    val partitionMaxSize = bucketSize * math.pow(2, currentIteration -1).toInt
    val (partitionIndex, edgeList) = partition
    val partitionStartNodeIndex: Int = partitionMaxSize * (partitionIndex-1)
    val partitionEndNodeIndex: Int = partitionMaxSize * partitionIndex
    val subgraphList = new mutable.MutableList[mutable.HashSet[String]]

    val firstEdge = edgeList.toSeq(0)
    val partitionStartFrameNum =
      if (firstEdge.metadata("index").toInt == partitionStartNodeIndex) firstEdge.srcNode.frameNum else -1

    val lastEdge = edgeList.toSeq(edgeList.size - 1)
    val partitionEndFrameNum =
      if (lastEdge.metadata("index").toInt == partitionEndNodeIndex) lastEdge.srcNode.frameNum else -1

    /** To have a key and a set of values, we use MultiMap */
    val edgeMap = new mutable.HashMap[String, MCCEdge]
    /** A map of all edges originating from the key (i.e the source node) */
    val srcNodeMap = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
    /** A map of edges originating or ending in the current node (key of the map) */
    val edgeMapString = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]


    for(edge <- edgeList) {
      val srcKey = edge.srcNode.hashKey()
      val destKey = edge.destNode.hashKey()
      edgeMap(edge.hashKey()) = edge
      srcNodeMap.addBinding(srcKey, edge.hashKey())
      edgeMapString.addBinding(srcKey, edge.hashKey())
      edgeMapString.addBinding(destKey, edge.hashKey())
    }

    /** Edges we need to carry forward to the next iteration */
    val filteredEdges = new mutable.HashSet[String]()

    val discardedEdges = new mutable.HashSet[String]()
    val visitedNodes = new mutable.HashSet[String]()

    for(node <- srcNodeMap.keys) {
      if (!visitedNodes.contains(node)) {
        val (length, edges, hasBorderNode) = getGraphInfo(node, 0, false, srcNodeMap, new mutable.HashSet[String](),
          partitionEndFrameNum.toString, partitionStartFrameNum.toString)

        /** If the graph has a border node then we store the entire graph containing it for the next iteration */
        if (hasBorderNode) {
          val (connectedEdges, visited) = findConnectedNodes(node, edgeMapString, new mutable.HashSet[String](),
            new mutable.HashSet[String]())
          visitedNodes ++= visited
          filteredEdges ++= connectedEdges
        }
        else {
          if (length >= minGraphLength) {
            val (connectedEdges, visited) = findConnectedNodes(node, edgeMapString, new mutable.HashSet[String](),
              new mutable.HashSet[String]())
            visitedNodes ++= visited
            subgraphList += connectedEdges
          }
          else {
            discardedEdges ++= edges
            logger.info(s"Iteration $currentIteration," +
              s"PartitionIndex: $partitionIndex," +
              s"Discarded Edges : ${edges}")
          }
        }
      }
    }
    logger.info(s"Iteration $currentIteration," +
      s"PartitionIndex: $partitionIndex," +
      s"Subgraph found : ${subgraphList}")

    if (filteredEdges.isEmpty) {
      logger.info(s"Iteration $currentIteration," +
        s"PartitionIndex: $partitionIndex," +
        s"No edges in FilteredEdges found")
      //      return (-1, filteredEdges).
      return (-1, (new mutable.MutableList[MCCEdge](), subgraphList))
    }
    val newIndex = if (partitionIndex%2==0) partitionIndex/2 else (1 + partitionIndex/2)
    logger.info(s"Sending to new partition, iteration : $currentIteration, edges: $filteredEdges")

    val returnedEdges = edgeMap.filter(x => {
      filteredEdges.contains(x._1)
    })
    return (newIndex, (returnedEdges.values, subgraphList))
  }

  /**
   * To get the max length of the subgraph, starting from the
   * source node to the farthest child.
   * Also return a boolean if the subgraph contains a border edge.
   * If it contains a border that means we need further investigation.
   * @param srcNode Root node to start traversal
   * @param length Length of the graph
   * @param borderNodeFlag To check if the graph contains a border node
   * @param edgeMap All the edges in the subgraph originating from the graph traversal
   *                 from the given srcNode
   * @return Tuple (length of graph, all edges in the graph, boolean value if the
   *         graph contains a border node)
   */
  def getGraphInfo(
      srcNode: String,
      length: Int,
      borderNodeFlag: Boolean,
      edgeMap: mutable.HashMap[String, mutable.Set[String]],
      edgeList: mutable.HashSet[String],
      endFrameNum: String, startFrame: String): (Int, mutable.HashSet[String], Boolean) = {
    var maxLength = length
    var hasBorderNode = borderNodeFlag
    if (edgeMap.contains(srcNode)) {
      for (outEdge <- edgeMap(srcNode)) {
        edgeList += outEdge
        hasBorderNode |= (srcNode.split(":")(0) == startFrame || srcNode.split(":")(0) == endFrameNum)
        val graphInfo = getGraphInfo(outEdge.split(",")(1), length + 1,
          hasBorderNode, edgeMap, edgeList, endFrameNum, startFrame)
        maxLength = if (maxLength < graphInfo._1) graphInfo._1 else maxLength
        hasBorderNode |= graphInfo._3
      }
    }
    return (maxLength, edgeList, hasBorderNode)
  }

  def findConnectedNodes(
      node: String,
      edgeMap: mutable.HashMap[String, mutable.Set[String]],
      edges: mutable.HashSet[String],
      visitedNodes: mutable.HashSet[String]): (mutable.HashSet[String], mutable.HashSet[String]) = {

    visitedNodes += node
    for (edge <- edgeMap(node)) {
      edges += edge
      val (src, dest) = (edge.split(",")(0), edge.split(",")(1))
      if (!visitedNodes.contains(src)) {
        findConnectedNodes(src, edgeMap, edges, visitedNodes)
      }
      if (!visitedNodes.contains(dest)) {
        findConnectedNodes(dest, edgeMap, edges, visitedNodes)
      }
    }
    return (edges, visitedNodes)
  }

  /**
   * Method to recursively generate subgraphs from the partitions
   * @param edgeList
   * @return Array[(Bucket#, (Edges, Subgraphs found))]
   */
  def findSubgraphsIteratively(
      edgeList: RDD[(Int, Iterable[MCCEdge])], iteration: Int,
      buckerSize: Int,
      minGraphLength: Int,
      sc: SparkContext): Array[(Int, Iterable[MCCEdge])] = {
    var iter = iteration
    def startProcessing(obj: RDD[(Int, (Iterable[MCCEdge]))], iter: Int):
    RDD[(Int, Iterable[MCCEdge])] = {
      obj.map(x => processEdgePartition(x, iter, minGraphLength, buckerSize))
        .filter({case (bucket, (edges, subgraphs)) =>
          val fw = new FileWriter("subgraphs.txt", true)
          for (edge <- subgraphs) {
            fw.write(edge.toString() + "\n")
          }
          fw.close()
          bucket != -1
        })
        .reduceByKey({case ((edges1, subgraphs1), (edges2, subgraphs2)) =>
          val merged = new mutable.HashSet[MCCEdge]()
          merged ++= edges1
          merged ++= edges2
          subgraphs1 ++= subgraphs2
          (merged, subgraphs1)
        })
        .map({case (bucket, (edges, subgraphs)) => (bucket, edges)})
    }

    var newGraph = startProcessing(edgeList, iter)

    iter += 1

    /** if edgeList is empty implies that all valid subgraphs were found */
    while (newGraph.count() > 1) {
      val tmp = startProcessing(newGraph, iter)
      newGraph = tmp
      iter += 1
      logger.debug(edgeList.toDebugString)
    }
    val tmp = startProcessing(newGraph, iter).collect()
    return tmp
  }

  /**
   *
   * @param edgeListRDD
   */
  def createPartitionIndex(edgeListRDD: RDD[MCCEdge]): RDD[MCCEdge] = {
    val temp = edgeListRDD.map(edge => (edge.srcNode.frameNum, List(edge)))
      .reduceByKey(_ ++ _)
      .sortBy(_._1)
      .zipWithIndex

    temp.flatMap({
      case ((key, edges), index) =>
        edges.map(edge => edge.updateMetadata("index", index.toString))
    })
  }

  /**
   * For each consecutive frame pair, find it's components.
   * For each component pairing, find if the element-wise
   * component pairing results in a zero matrix.
   * If not output a new edge pairing of the form ((Frame, Component), (Frame, Component))
   *
   * Note : findEdges assumes that all SciDatasets have an attribute called "FRAME" which
   * records the frame number.
   *
   * @param sRDD the input RDD of SciDataset pairs
   * @param varName the name of the variable being used
   * @param maxAreaOverlapThreshold the maximum area over lap threshold
   * @param minAreaOverlapThreshold the minimum area overlap threhshold
   * @param convectiveFraction convective fraction threshold
   * @param minArea the minimum area to check for third weight
   * @param nodeMinArea the minimum area of a component
   * @return
   */
  def findEdges(
      sRDD: RDD[(SciDataset, SciDataset)],
      varName: String,
      maxAreaOverlapThreshold: Double,
      minAreaOverlapThreshold: Double,
      convectiveFraction: Double,
      minArea: Int,
      nodeMinArea: Int): RDD[MCCEdge] = {

    sRDD.flatMap({
      case (sd1, sd2) =>
        val (t1, t2) = (sd1(varName), sd2(varName))
        val (frame1, frame2) = (sd1.attr("FRAME"), sd2.attr("FRAME"))
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
        val (components1, _) = t1().labelComponents
        val (components2, _) = t2().labelComponents
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

        for (row <- 0 until product.rows) {
          for (col <- 0 until product.cols) {
            /** Find non-zero points in product array */
            MCCOps.updateComponent(components1(row, col), frame1, t1()(row, col), row, col, nodeMap)
            MCCOps.updateComponent(components2(row, col), frame2, t2()(row, col), row, col, nodeMap)
            if (product(row, col) != 0.0) {

              /** If overlap exists create an edge and update overlapped area */
              val label1 = components1(row, col)
              val node1 = nodeMap(frame1 + ":" + label1)

              val label2 = components2(row, col)
              val node2 = nodeMap(frame2 + ":" + label2)

              val edgeKey = s"$frame1:$label1,$frame2:$label2"
              val edge = MCCEdgeMap.getOrElse(edgeKey, new MCCEdge(node1, node2))
              edge.incrementAreaOverlap()
              MCCEdgeMap(edgeKey) = edge
            }
          }
        }

        MCCOps.updateEdgeMapCriteria(MCCEdgeMap, maxAreaOverlapThreshold, minAreaOverlapThreshold,
          convectiveFraction, minArea, nodeMinArea)
    })
  }

  /**
   * Function that adds weights on the edges
   *
   * @param MCCEdgeMap mutable.HashMap[String, MCCEdge] of the current edgeMap
   * @param maxAreaOverlapThreshold the maximum area over lap threshold
   * @param minAreaOverlapThreshold the minimum area overlap threhshold
   * @param convectiveFraction convective fraction threshold
   * @param minArea the minimum area to check for third weight
   * @param nodeMinArea the minimum area of a component
   * @return Iterable[org.dia.algorithms.mcc.MCCEdge] of weighted edges
   */
  def updateEdgeMapCriteria(
      MCCEdgeMap: mutable.HashMap[String, MCCEdge],
      maxAreaOverlapThreshold: Double,
      minAreaOverlapThreshold: Double,
      convectiveFraction: Double,
      minArea: Int,
      nodeMinArea: Int): Iterable[org.dia.algorithms.mcc.MCCEdge] = {

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
  }

  /**
   * Function that adds node to the nodeMap according the label sciTensor
   *
   * @param label Double representing the label number in the sciTensor
   * @param frame String representing the datetime
   * @param value Double representing the variable value at the row,col
   * @param row Int
   * @param col Int
   * @param nodeMap mutable.HashMap[String, MCCNode]
   * @return Iterable[org.dia.algorithms.mcc.MCCEdge] of weighted edges
   */
  def updateComponent(
      label: Double,
      frame: String,
      value: Double,
      row: Int,
      col: Int,
      nodeMap: mutable.HashMap[String, MCCNode]): Unit = {
    if (label != 0.0) {
      val node = nodeMap.getOrElse(frame + ":" + label, new MCCNode(frame, label))
      node.updateNodeData(value, row, col)
      nodeMap(frame + ":" + label) = node
    }
  }

  /**
   * Collect the edges of the form ((String, Double), (String, Double))
   * From the edges collect all used vertices.
   * Repeated vertices are eliminated due to the set conversion.
   * @param MCCEdgeList Collection of MCCEdges
   * @param MCCNodeMap Dictionary of all the MCCNodes
   */
  def processEdges(MCCEdgeList: Iterable[MCCEdge], MCCNodeMap: mutable.HashMap[String, MCCNode]): Unit = {
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

  /**
   * To create a map of Nodes from the edges found.
   *
   * @param edges sequence of MCCEdge objects
   * @param lat the lattitude dimension array
   * @param lon the longitude dimension array
   * @return
   */
  def createNodeMapFromEdgeList(
      edges: Seq[MCCEdge],
      lat: Array[Double],
      lon: Array[Double]): mutable.HashMap[String, MCCNode] = {

    val MCCNodes = edges.flatMap(edge => List(edge.srcNode, edge.destNode)).distinct
    val MCCNodeKeyValuesSet = MCCNodes.map(node => {
      val key = node.hashKey()
      node.updateLatLon(lat, lon)
      (key, node)
    })
    mutable.HashMap[String, MCCNode](MCCNodeKeyValuesSet: _*)
  }

  /**
   * Records the frame number in all SciDataset stored in the RDD
   * Preconditon : The files read are are of the form merg_XX_4km-pixel.nc
   *
   * @param sRDD input RDD of SciDatasets
   * @param varName the variable being used
   * @return RDD of SciDataset with Frame number recorded in metadata table
   */
  def recordFrameNumber(sRDD: RDD[SciDataset], varName: String): RDD[SciDataset] = {
    sRDD.map(p => {
      val FrameID = p.datasetName.split("_")(1).toInt
      p("FRAME") = FrameID.toString
      p(varName) = p(varName)(0)
    })
  }

}
