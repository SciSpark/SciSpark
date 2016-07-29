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

import java.io.{BufferedWriter, OutputStreamWriter}
import breeze.io.TextWriter.FileWriter
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.dia.core.SciSparkContext
import org.slf4j.Logger

import scala.collection.mutable

/**
  * Implements MCC via a distributed graph approach.
  *
  * Currently, the input data is a file in the form
  * of an Edgelist generated is an output by
  * MainNetcdfDFSMCC
  */
object MainDistGraphMCC {

  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  val bins: Int = 8
  val frames: Int = 40
  val frameBucketSize: Float = frames / bins
  val minAcceptableFeatureLength = 3
  val maxIterations = scala.math.ceil(scala.math.log(bins)/scala.math.log(2)) + 1
  /*
  This is the function which recursively creates subgraphs and merges partitions
  until we are left with one partition or no edges to process.
   */
  def getSubgraphs(graph: RDD[(Integer, Iterable[MCCEdge])], iter: Int): Unit = {
    logger.info(s"Current iteration : $iter, Max iterations : $maxIterations")
    logger.info(s"Graph: $graph")
    if (graph.isEmpty() || graph.count() <=1) {
      logger.info("Graph empty exiting")
      logger.info(s"Graph count ${graph.count()}")
      graph.foreach(x => {
        println(x._2)
      })
      return
    }
    logger.info(s"Graph count ${graph.count()}")
    val newGraph = graph.map(x => createPartialGraphs(x._1, x._2, iter))
      .filter(x => x._1 != -1)
        .reduceByKey((x,y) => reduceEdgeListsByKey(x, y))

    /*
    Performing a count everytime would be expensive.
    Letting the graph.isEmpty condition to take care of the exit from this loop.
    Keeping the code here, if the above does not work as expected
     */
    //      if(graph.count() == 1){e
    //        return graph
    //      }
    println(graph.toDebugString)
    getSubgraphs(newGraph, iter+1)

  }

  def reduceEdgeListsByKey(edge1: Iterable[MCCEdge], edge2: Iterable[MCCEdge]): Iterable[MCCEdge] = {
    val mergedEdgelists = new mutable.HashSet[MCCEdge]();
    for(edge <- edge1){
      mergedEdgelists += edge
    }
    for(edge <- edge2) {
      mergedEdgelists += edge
    }
    return mergedEdgelists
  }

  /*
  Converting an Iterable[Iterable[MCCEdge]] to Iterable[MCCEdge]
   */
  def flattenSets(bucket: Integer, edgeList: Iterable[Iterable[MCCEdge]]): (Integer, Iterable[MCCEdge]) = {
    val collapsedEdgeList = new mutable.HashSet[MCCEdge]
    for (edgeSet <- edgeList) {
      collapsedEdgeList ++= edgeSet
    }
    return (bucket, collapsedEdgeList)
  }

  /*
  This function performs the mapping and filtering of subgraphs.
  Given a bucketId and list of egdes, it finds filters out subgraphs
  and any edges not meeting a given criteria (defined by min path length for a subgraph).
  It then returns a new mapping to perform merging of two consecutive
  partitions of buckets no overlap.

  Ex - Input bucketIds - 1,2,3,4
       Output would contain 2 bucketIds - 1(1,2) and 2(3,4)
   */
  def createPartialGraphs(bucket: Integer,
                          edgeList: Iterable[MCCEdge],
                          iteration: Int): (Integer, Iterable[MCCEdge]) = {

    logger.info(s"Processing partial graph for bucket:$bucket iteration:$iteration")
    val edgeMap = new mutable.HashMap[String, mutable.Set[MCCEdge]] with mutable.MultiMap[String, MCCEdge]
    val nodeMap = new mutable.HashMap[String, MCCNode]() // for faster lookups for nodes
    val currentFrameChunkSize = frameBucketSize*iteration
    val bucketStartFrame = bucket * currentFrameChunkSize - currentFrameChunkSize + 1 //The first frame number in the bucket
    val bucketEndFrame = bucket * currentFrameChunkSize // last frame in bucket
    var minFrame = Integer.MAX_VALUE
    var maxFrame = 0

    for (edge <- edgeList) {
      // Keying on concatenation of FrameCloudElemNum, Ex - given F:12 CE: 3.0 => "123.0"
      edgeMap.addBinding(edge.srcNode.frameNum + "" + edge.srcNode.cloudElemNum, edge)
      nodeMap.put(edge.srcNode.toString(), edge.srcNode)
      nodeMap.put(edge.destNode.toString(), edge.destNode)
      if (edge.srcNode.frameNum > maxFrame) {
        maxFrame = edge.srcNode.frameNum
      }
      if (edge.srcNode.frameNum < minFrame) {
        minFrame = edge.srcNode.frameNum
      }
    }
    // Building the partial graphs
    val nodeSet = new mutable.HashSet[MCCNode]()
    val borderNodes = new mutable.HashSet[MCCNode]()
    val borderEdges = new mutable.HashSet[MCCEdge]()

    for (edge <- edgeList) {
      val destNodeKey: String = edge.destNode.frameNum + "" + edge.destNode.cloudElemNum
      var srcNode: MCCNode = nodeMap.get(edge.srcNode.toString()).get
      var destNode: MCCNode = nodeMap.get(edge.destNode.toString()).get

      nodeSet += srcNode
      srcNode.addOutgoingEdge(edge)
      destNode.addIncomingEdge(edge)

      /*
      If destNode does not exist in the NodeMap, we can infer that
      this edge is originating from the last frame in this bucket.
      Or if this is the start frame of the partition
       */
      if (srcNode.frameNum == bucketStartFrame || srcNode.frameNum == bucketEndFrame) {
        if ((bucket == 1 && srcNode.frameNum == bucketStartFrame)) {
          // Do not add as this is the First frame of the graph and there cannot be any incoming edges to this
        }
        else {
          borderEdges += edge
          borderNodes += srcNode
        }
      }
    }

    // Finding all source nodes from the node set
    val sourceNodeSet = new mutable.HashSet[MCCNode]()
    nodeSet.foreach(node => {
      if(node.inEdges.size<1){
        sourceNodeSet += node
      }
    })

    val filteredEdgeList = new mutable.HashSet[MCCEdge]
    val subgraphList = new mutable.MutableList[mutable.HashSet[MCCEdge]]
    val discardedEdges = new mutable.HashSet[MCCEdge]

    if (!sourceNodeSet.isEmpty) {
      for (node <- sourceNodeSet) {
        val result = getSubgraphLenth(node, 0, new mutable.HashSet[MCCEdge], nodeMap, false, bucket, borderNodes)
        //If the source node is a border node, add its entire subgraph as it needs further investigation
        if (borderNodes.contains(node)) {
          filteredEdgeList ++= result._2
        }
        // If the subgraph contains a border edge, add it to filteredEdges for further investigation
        else if (result._3 == true) {
          filteredEdgeList ++= result._2
        }
        /* If the subgraph is entirely contained within the bounds of the partition,
        then check for feature length and write to file or discard accordingly
        */
        else if (result._1 > minAcceptableFeatureLength && !result._3) {
          subgraphList += result._2
          printGraphForMatplotLob(s"Iteration $iteration from BID: $bucket Subgraph discovered", result._2, iteration, bucket)

        }
        else {
          discardedEdges ++= result._2
          printGraphForMatplotLob(s"Iteration $iteration from BID: $bucket Discarded edges", result._2, iteration, bucket)
        }

      }
    }

    printGraphForMatplotLob(s"Iteration $iteration from BID: $bucket Next Round",filteredEdgeList, iteration, bucket)
    if (filteredEdgeList.isEmpty) {
      return (-1, filteredEdgeList)
    }
    val newBucket = if(bucket%2==0) bucket/2 else (1 + bucket/2)
    logger.info(s"Sending to new bucket:$newBucket from BID:$bucket iteration:$iteration")
    return (newBucket, filteredEdgeList)
  }

  /*
    To get the max length of the subgraph, starting from the
    source node to the farthest child.
    Also return a boolean if the subgraph contains a border edge.
    If it contains a border that means we need further investigation.
   */
  def getSubgraphLenth(source: MCCNode, length: Int, edges: mutable.HashSet[MCCEdge],
                       nodeMap: mutable.HashMap[String, MCCNode],
                       containsBorderEdge: Boolean,
                       bucket: Int,
                       borderNodes: mutable.HashSet[MCCNode]):
  (Int, mutable.HashSet[MCCEdge], Boolean) = {
    var maxLength = length
    var hasBorderEdge = containsBorderEdge
    if (source == Nil)
      return (maxLength, edges, hasBorderEdge)
    else {
      for (outEdge: MCCEdge <- source.outEdges) {
        edges += outEdge
        hasBorderEdge = if (hasBorderEdge) hasBorderEdge else borderNodes.contains(source)
        val childNode = nodeMap.get(outEdge.destNode.toString()).get
        val l = getSubgraphLenth(childNode, length + 1, edges, nodeMap,
          hasBorderEdge, bucket, borderNodes)
        if (maxLength < l._1) {
          maxLength = l._1
        }
        hasBorderEdge = if(containsBorderEdge) containsBorderEdge else l._3
      }
    }
    return (maxLength, edges, hasBorderEdge)
  }

  /*
  For debugging purposes
   */
  def printGraphForMatplotLob(label:String, edges: mutable.HashSet[MCCEdge],iteration:Int, bucket:Int) = {
    println(s"Matplotlib: $label ##$edges")
  }

  def mapFrames(x: String): (Integer, MCCEdge) = {
    logger.debug("Mapping frames to partitions")
    val nodes = x.split("\\),\\(")
    val source = nodes(0).slice(2, nodes(0).length).split(",") // (FrameNum, CloudElemNum)
    val dest = nodes(1).slice(0, nodes(1).length - 2).split(",") // (FrameNum, CloudElemNum)
    val nodeMap = new mutable.HashMap[String, MCCNode]()
    val sourceFrameNum = x.slice(2, x.indexOfSlice(",")).toInt
    val sourceKey = source(0) + source(1)
    val destKey = dest(0) + dest(1)
    if (!nodeMap.contains(sourceKey)) {
      nodeMap.put(sourceKey, new MCCNode(source(0).toInt, source(1).toFloat))
    }
    if (!nodeMap.contains(destKey)) {
      nodeMap.put(destKey, new MCCNode(dest(0).toInt, dest(1).toFloat))
    }
    val sourceNode: MCCNode = nodeMap.get(sourceKey).get
    val destNode = nodeMap.get(destKey).get

    for (i <- 1 to bins) {
      if (sourceFrameNum <= frameBucketSize * i) {
        val bucket: Int = i
        return (bucket,
          new MCCEdge(sourceNode, destNode))
      }
    }
    return (bins,
      new MCCEdge(sourceNode, destNode)
      )
  }

  def mapEdgesToBuckets(edge: MCCEdge): (Integer, MCCEdge) = {
    val sourceFrameNum = edge.srcNode.frameNum
    val bucket = scala.math.ceil(sourceFrameNum/frameBucketSize).toInt
    return (bucket, edge)
  }

  def mapEdgesToBuckets(edge: ((String, Double), (String, Double), Int)): (Integer, MCCEdge) = {
    val sourceNode = new MCCNode(edge._1._1.toInt, edge._1._2)
    val destNode = new MCCNode(edge._2._1.toInt, edge._2._2)
    val mccEdge = new MCCEdge(sourceNode, destNode, edge._3)
    val sourceFrameNum = mccEdge.srcNode.frameNum
    val bucket = scala.math.ceil(sourceFrameNum/frameBucketSize).toInt
    return (bucket, mccEdge)
  }

//   def performMCCfromRDD(edgelist : RDD[MCCEdge]):Unit = {
//     val graph = edgelist.map(mapEdgesToBuckets)
//       .groupByKey()
//     getSubgraphs(graph, 1)
//   }

  def performMCCfromRDD(edgelist : RDD[((String, Double), (String, Double), Int)]):Unit = {
    val graph = edgelist.map(mapEdgesToBuckets)
      .groupByKey()
      .collect()
//    getSubgraphs(graph, 1)
  }

  def main(args: Array[String]) {
    /**
      * Input arguments to the program :
      * args(0) - the spark master URL. Example : spark://HOST_NAME:7077
      * args(1) - the number of desired partitions. Default : 2
      * args(3) - local path to files
      *
      */
    val masterURL = if (args.length < 1) "local[2]" else args(0)
    val partCount = if (args.length < 2) 2 else args(1).toInt
    val hdfspath = if (args.length < 3) "resources/graph/graphEdges" else args(2)

    /**
      * Initialize the spark context to point to the master URL
      */
    val sparkConf = new SparkConf()
      .setMaster(masterURL)
      .setAppName("DGTG : Distributed MCC Search")
    val sc = new SparkContext(sparkConf)

    val RDD = sc.textFile(hdfspath)
    val count = RDD.flatMap(line => line.split(", "))
      .map(mapFrames)
      .groupByKey()
    println(count.toDebugString)
    getSubgraphs(count, 1)
    logger.info("MCC Graph computation complete")
  }
}