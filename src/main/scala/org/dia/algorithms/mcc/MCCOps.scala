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
import java.util

import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.dia.core.SciTensor
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
   * @param partition Tuple containing the bucket# and list of edges
   * @param currentIteration Current iteration number
   * @param bucketSize Number of frames in each bucket
   * @param outputDir Path to store subgraphs found
   * @return
   */
  def processEdgePartition(
      partition: (Int, Iterable[MCCEdge]),
      currentIteration: Int,
      minGraphLength: Int,
      bucketSize: Int,
      outputDir: String): (Int, Iterable[MCCEdge]) = {

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
    writeSubgraphsToFile(subgraphList, outputDir, currentIteration, partitionIndex)

    val newIndex = if (partitionIndex%2==0) partitionIndex/2 else (1 + partitionIndex/2)
    logger.info(s"Sending to new partition, iteration : $currentIteration, edges: $filteredEdges")

    if (filteredEdges.isEmpty) {
      logger.info(s"Iteration $currentIteration," +
        s"PartitionIndex: $partitionIndex," +
        s"No edges in FilteredEdges found")
      //      return (-1, filteredEdges).
      return (newIndex, new mutable.MutableList[MCCEdge]())
    }

    val returnedEdges = edgeMap.filter(x => {
      filteredEdges.contains(x._1)
    })
    return (newIndex, returnedEdges.values)
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

  /**
   * Find all nodes connected to the given node
   * @param node Source node to start search
   * @param edgeMap A map of edges(value) originating or ending at a given node(key)
   * @param edges A list of edges connect to source node
   * @param visitedNodes A list of visited nodes
   * @return
   */
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

  private def writeSubgraphsToFile(
      subgraphList: Iterable[mutable.HashSet[String]],
      outputDir: String,
      iteration: Int,
      bucket: Int): Unit = {

    val filepath = new Path(outputDir + System.getProperty("file.separator") +
      s"subgraphs-${iteration}-${bucket}.txt")
    val conf = new Configuration()
    val fs = FileSystem.get(filepath.toUri, conf)
    val os = fs.create(filepath)
    for (edge <- subgraphList) {
      os.write((edge.toString() + "\n").getBytes())
    }
    os.close()
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
      outputDir: String): Array[(Int, Iterable[MCCEdge])] = {
    var iter = iteration
    def startProcessing(obj: RDD[(Int, (Iterable[MCCEdge]))], iter: Int):
    RDD[(Int, Iterable[MCCEdge])] = {
      obj.map(x => processEdgePartition(x, iter, minGraphLength, buckerSize, outputDir))
        .reduceByKey({case (edges1, edges2) =>
          val merged = new mutable.HashSet[MCCEdge]()
          merged ++= edges1
          merged ++= edges2
          (merged)
        })
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
}
