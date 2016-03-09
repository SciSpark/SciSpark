package org.dia.algorithms.mcc


import java.io.{FileOutputStream, OutputStreamWriter, BufferedWriter, File}
import breeze.io.TextWriter.FileWriter
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

  val bins: Int = 100
  val frames = 7000
  val frameChunkSize = 70
  val minAcceptableFeatureLength = 3

  /*
  This is the function which recursively creates subgraphs and merges partitions
  untill we are left with one partition or no edges to process.
   */
  def getSubgraphs(graph: RDD[(Integer, Iterable[MCCEdge])]): Unit = {

    if (graph.isEmpty()) {
      return
    }
    val newGraph = graph.map(x => createPartialGraphs(x._1, x._2))
      .filter(x => x._1 != -1)
      .groupByKey()
      .map(x => flattenSets(x._1, x._2))

    /*
    Performing a count everytime would be expensive.
    Letting the graph.isEmpty condition to take care of the exit from this loop.
    Keeping the code here, if the above does not work as expected
     */
    //      if(graph.count() == 1){e
    //        return graph
    //      }

    return getSubgraphs(newGraph)
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
  def createPartialGraphs(bucket: Integer, edgeList: Iterable[MCCEdge]): (Integer, Iterable[MCCEdge]) = {

    println(s"Initial edgelist from bucket $bucket" + edgeList)
    val nodeMap = new mutable.HashMap[String, mutable.Set[MCCEdge]] with mutable.MultiMap[String, MCCEdge]

    val bucketStartFrame = bucket * frameChunkSize - frameChunkSize + 1 //The first frame numner in the bucket
    var minFrame = Integer.MAX_VALUE
    var maxFrame = 0
    for (edge <- edgeList) {
      // Keying on concatenation of FrameCloudElemNum, Ex - given F:12 CE: 3.0 => "123.0"
      nodeMap.addBinding(edge.srcNode.frameNum + "" + edge.srcNode.cloudElemNum, edge)
      if (edge.srcNode.frameNum > maxFrame) {
        maxFrame = edge.srcNode.frameNum
      }
      if (edge.srcNode.frameNum < minFrame) {
        minFrame = edge.srcNode.frameNum
      }
    }

    // Building the partial graphs
    val nodeSet = new mutable.HashSet[MCCNode]
    val borderNodes = new mutable.HashSet[MCCNode]
    val borderEdges = new mutable.HashSet[MCCEdge]
    nodeMap.foreach((node: (String, mutable.Set[MCCEdge])) => {
      for (edge <- node._2) {
        val destNodeKey: String = edge.destNode.frameNum + "" + edge.destNode.cloudElemNum
        nodeSet += edge.srcNode
        edge.srcNode.addOutgoingEdge(edge)
        edge.destNode.addIncomingEdge(edge)
        /*
        If destNode does not exist in the NodeMap, we can infer that
        this edge is originating from the last frame in this bucket.
        Or if this is the start frame of the partition
         */
        if (!nodeMap.contains(destNodeKey) || edge.srcNode.frameNum == bucketStartFrame) {
          borderEdges += edge
          borderNodes += edge.srcNode
        }
      }
    })
    // Finding all source nodes from the node set
    val sourceNodeSet = nodeSet.filter(node => !node.inEdges.isEmpty)

    val filteredEdgeList = new mutable.HashSet[MCCEdge]
    val out = new BufferedWriter(
      new OutputStreamWriter(new FileOutputStream(new File("intermediate.txt"))))
    if (!sourceNodeSet.isEmpty) {
      for (node <- sourceNodeSet) {
        val result = getSubgraphLenth(node, 0, new mutable.HashSet[MCCEdge], borderEdges, false)

        //If the source node is a border node, add its entire subgraph as it needs further investigation
        if (borderNodes.contains(node)) {
          filteredEdgeList ++= result._2
        }
        // If the subgraph contains a border edge, add it to filteredEdges for further investigation
        else if (result._3) {
          filteredEdgeList ++= result._2
        }
        /* If the subgraph is entirely contained within the bounds of the partition,
        then check for feature length and write to file or discard accordingly
        */
        else if (result._1 > minAcceptableFeatureLength) {
          //DEBUG
          println(s"Subgraphs filtered from $bucket: " + result._2)
          out.write(result._2.toString())
        }
        else {
          //DEBUGGING
          println(s"Edges that don't meet above criteria from bucket $bucket : " + result._2)
        }
        out.newLine()
      }
    }
    out.close()
    println(s"Edgelist for next round from bucket $bucket" + filteredEdgeList)
    if (filteredEdgeList.isEmpty) {
      return (-1, filteredEdgeList)
    }
    return (1 + bucket / (frameChunkSize * 2), filteredEdgeList)
  }

  /*
    To get the max length of the subgraph, starting from the
    source node to the farthest child.
    Also return a boolean if the subgraph contains a border edge.
    If it contains a border that means we need further investigation.
   */
  def getSubgraphLenth(source: MCCNode, length: Int, edges: mutable.HashSet[MCCEdge], borderEdges: mutable.HashSet[MCCEdge], containsBorderEdge: Boolean):
  (Int, mutable.HashSet[MCCEdge], Boolean) = {
    var maxLength = length
    if (source == Nil)
      return (maxLength, edges, containsBorderEdge)
    else {
      for (outEdge: MCCEdge <- source.outEdges) {
        edges += outEdge
        val l = getSubgraphLenth(outEdge.destNode, length + 1, edges, borderEdges, borderEdges.contains(outEdge))
        if (maxLength < l._1) {
          maxLength = l._1
        }
      }
    }
    return (maxLength, edges, containsBorderEdge)
  }

  def mapFrames(x: String): (Integer, MCCEdge) = {

    val nodes = x.split("\\),\\(")
    val source = nodes(0).slice(2, nodes(0).length).split(",")
    val dest = nodes(1).slice(0, nodes(1).length - 2).split(",")

    val sourceFrameNum = x.slice(2, x.indexOfSlice(",")).toInt
    for (i <- 1 to bins) {
      if (sourceFrameNum < frameChunkSize * i) {
        val bucket: Int = i
        return (bucket,
          new MCCEdge(new MCCNode(source(0).toInt, source(1).toFloat), //creating an edge from String
            new MCCNode(dest(0).toInt, dest(1).toFloat)))
      }
    }
    return (bins,
      new MCCEdge(new MCCNode(source(0).toInt, source(1).toFloat),
        new MCCNode(dest(0).toInt, dest(1).toFloat))
      )
  }

  def main(args: Array[String]) {
    /**
      * Input arguments to the program :
      * args(0) - the spark master URL. Example : spark://HOST_NAME:7077
      * args(1) - the number of desired partitions. Default : 2
      * args(3) - local path to files
      *
      */
    val masterURL = if (args.length <= 1) "local[4]" else args(0)
    val partCount = if (args.length <= 2) 2 else args(1).toInt
    val hdfspath = if (args.length <= 3) "resources/graph/EdgeList.txt" else args(2)

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
    getSubgraphs(count)
    count.saveAsTextFile("MCCDistGraphOutput")
    println("Done")
  }
}