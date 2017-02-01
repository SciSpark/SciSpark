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
package org.dia.apps

import scala.collection.mutable

import org.apache.spark.rdd.RDD

import org.dia.algorithms.mcs._
import org.dia.core.{SciDataset, SciSparkContext, SRDDFunctions}
import org.dia.tensors.AbstractTensor
import org.dia.utils.{FileUtils, WWLLNUtils}

object MCSApp extends App {

  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  /**
   * Process cmd line arguments
   */
  val masterURL = if (args.isEmpty) "local[*]" else args(0)
  val partitions = if (args.length <= 1) 8 else args(1).toInt
  val path = if (args.length <= 2) "resources/paperSize/" else args(2)
  val varName = if (args.length <= 3) "ch4" else args(3)
  val outputLoc = if (args.length <= 4) "output" else args(4)
  val WWLLNpath = "resources/WWLLN/"

  /**
   * User parameters for the algorithm itself
   */
  val maxAreaOverlapThreshold = 0.65
  val minAreaOverlapThreshold = 0.50
  val outerTemp = 241.0
  val innerTemp = 233.0
  val convectiveFraction = 0.9
  val minArea = 625
  val nodeMinArea = 150
  val minAreaThres = 16
  val minGraphLength = 4

  logger.info("Starting MCS")

  val outputDir = FileUtils.checkHDFSWrite(outputLoc)

  /**
   * Initialize the spark context to point to the master URL
   */
  val sc = new SciSparkContext(masterURL, "DGTG : Distributed MCS Search")

  /**
   * Get WWLLN data to update MCSNodes
   */
  val broadcastedWWLLNDF = sc.readWWLLNData(WWLLNpath, partitions)
  logger.info("Check the WWLLN data read in \n" + broadcastedWWLLNDF.value.show(10))

  /**
   * Initialize variableName to avoid serialization issues
   */
  val variableName = varName

  /**
   * Ingest the input file and construct the SRDD.
   * For MCS the sources are used to map date-indexes.
   * The metadata variable "FRAME" corresponds to an index.
   * The indices themselves are numbered with respect to
   * date-sorted order.
   *
   * Note if no HDFS path is given, then randomly generated matrices are used.
   *
   */
  val sRDD = sc.sciDatasets(path, List(varName, "longitude", "latitude"), partitions)

  /**
   * Collect lat and lon arrays
   */
  val sampleDataset = sRDD.take(1)(0)
  val lon = sampleDataset("longitude").data()
  val lat = sampleDataset("latitude").data()

  /**
   * Record the frame Number in each SciTensor
   */
  val labeled = MCSOps.recordFrameNumber(sRDD, variableName)

  /**
   * Filter for temperature values under 241.0
   */
  val filtered = labeled.map(p => p(variableName) = p(variableName) <= 241.0)

  /**
   * Pair consecutive frames
   */
  val consecFrames = SRDDFunctions.fromRDD(filtered).pairConsecutiveFrames("FRAME")

  /**
   * Create the graph
   */
  val edgeListRDD = MCSOps.findEdges(consecFrames,
    variableName,
    maxAreaOverlapThreshold,
    minAreaOverlapThreshold,
    convectiveFraction,
    minArea,
    nodeMinArea,
    minAreaThres)

  edgeListRDD.cache()
  edgeListRDD.localCheckpoint()

  /**
   * Collect the edgeList and construct NodeMap that contains the node metadata
   */
  val MCSEdgeList = edgeListRDD.collect()
  val MCSNodeMap = MCSOps.createNodeMapFromEdgeList(MCSEdgeList, lat, lon)
  val broadcastedNodeMap = sc.sparkContext.broadcast(MCSNodeMap)

  /**
   * Optional add ons
   */
  MCSEdgeList.foreach(edge => {
    // Add WWLLN data to nodes
    MCSUtils.addWWLLN(edge, broadcastedNodeMap, broadcastedWWLLNDF)
    // Generate the netcdfs
    MCSUtils.writeEdgeNodesToNetCDF(edge, broadcastedNodeMap, lat, lon, false, "/tmp", null)
  })

  /**
   * Write Nodes and Edges to disk
   */
  logger.info("NUM VERTICES : " + MCSNodeMap.size + "\n")
  logger.info("NUM EDGES : " + MCSEdgeList.size + "\n")

  val MCSNodeFilename: String = outputDir + System.getProperty("file.separator") + "MCSNodes.json"
  MCSUtils.writeNodesToFile(MCSNodeFilename, MCSNodeMap.values)

  val MCSEdgeFilename: String = outputDir + System.getProperty("file.separator") + "MCSEdges.txt"
  MCSUtils.writeEdgesToFile(MCSEdgeFilename, MCSEdgeList)

  /**
   * Find the subgraphs
   */
  val edgeListRDDIndexed = MCSOps.createPartitionIndex(edgeListRDD)
  val count = edgeListRDDIndexed.count.toInt
  val buckets = 4
  val maxParitionSize = count / buckets
  val subgraphs = edgeListRDDIndexed
    .map(MCSOps.mapEdgesToBuckets(_, maxParitionSize, buckets))
    .groupByKey()
  val subgraphsFound = MCSOps.findSubgraphsIteratively(subgraphs, 1, maxParitionSize,
    minGraphLength, outputDir)
  for(x <- subgraphsFound) {
    logger.info("Edges remaning : " + x._2.toList)
  }

  /**
   * Load subgraphs from file into RDD
   */
  val subgraphsPath = outputDir + System.getProperty("file.separator") + "subgraphs-*"
  val subgraphsRDD = MCCOps.loadSubgraphsFromFile(subgraphsPath, sc.sparkContext)

  /**
   * Remove the nodes that are not in the subgraphs from the nodemap
   */
  val subgraphsNodes = subgraphsRDD.collect().flatten.flatMap{ case(a, b) => List(a, b)}
  val notSubgraphNodes = MCSNodeMap.keys.filterNot(subgraphsNodes.toSet).toList
  MCSNodeMap --= notSubgraphNodes

  /**
   * Add the criteria according to Goyens et al. 2011
   */
  val areasTempsAllRDD = subgraphsRDD.map(x => (MCSOps.getMCSAreas(x, broadcastedNodeMap),
    MCSOps.getMCS210KTemps(x, broadcastedNodeMap)))

  val areasTempsRDD = areasTempsAllRDD.map{ x =>
    val areaTempList = new mutable.ListBuffer[(String, Double, Double, List[String])]
    for (i <- 0 to x._1.length - 1) {
      areaTempList += ((x._1(i)._1, x._1(i)._2, x._2(i)._2, x._1(i)._3))
    }
    areaTempList.toList
  }

  /**
   * Add the stages
   */
  val findTheStages = areasTempsRDD.map(x => MCSOps.getDevStage(x))


  /**
   * Update the MCSNodeMap
   */
  val allStages = findTheStages.collect().flatten
  allStages.foreach{println}
  for (x <- 0 to allStages.length - 1) {
    for (nodeStr <- allStages(x)._3) {
      MCSNodeMap(nodeStr).updateDevStage(allStages(x)._2)
    }
  }

  val removeNodes = findTheStages.map{ x =>
    var count = 0
    var mcs: Boolean = false
    var rNodes = new mutable.ListBuffer[List[String]]
    for(i <- 1 to x.length - 1) {
      rNodes += x(i)._3
      count = if (x(i)._2 == "M") count + 1 else 0
      if (count >= 3) {
        mcs = true
      }
    }
    if (mcs) {
      rNodes.remove(0, rNodes.length)
    } else {
      rNodes += x(0)._3
    }
    rNodes.flatten.toList
  }

  /**
   * Update MCSNode map directly
   */
  MCSNodeMap --= removeNodes.collect().toList.flatten
  for ((k, v) <- MCSNodeMap) printf("%s,\t%s\t%s\t%s\t%s\n", k,
    v.getCenterLat(), v.getCenterLon, v.getArea(), v.getDevStage())

  /**
   * Remove broadcasted variables
   */
  broadcastedWWLLNDF.destroy()
  broadcastedNodeMap.destroy()

  /**
   * Elegantly stop the SciSpark Context
   */
  sc.stop

}
