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
  val WWLLNpath = "resources/WWLLN/" // must be hdfs file location

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

  /** Criteria for MCC */
  val minFeatureLength = 6
  val maxFeatureLEngth = 24
  val areaCriteriaA = 30000/16
  val b = 80000/16
  val tempA = 213.0f
  val tempB = 233.0f

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

  /** find potential nodes */
  val potentialNodes = sc.sparkContext.parallelize(MCSNodeMap.values.toSeq)
    .map(MCCOps.isPotentialNode(_, areaCriteriaA, b, tempA, tempB))
    .filter(x => x._1)
    .groupByKey()
    .collect()

  val potentialNodeSet = new mutable.HashSet[String]()
  for ((isCriteria, nodes) <- potentialNodes) {
    if (isCriteria) {
      potentialNodeSet ++= nodes
    }
  }

  val broadcastPotentialNodes = sc.sparkContext.broadcast(potentialNodeSet)
  val subgraphsPath = outputDir + System.getProperty("file.separator") + "subgraphs-*"
  val subgraphsRDD = MCCOps.loadSubgraphsFromFile(subgraphsPath, sc.sparkContext)

  println(subgraphsRDD.collect())

  /** Find MCCs in the subgraphs */
  val MCCs = subgraphsRDD.map(MCCOps.findMCC(_, minFeatureLength, maxFeatureLEngth, broadcastPotentialNodes))
    .reduce((x, y) => x ++ y)

  /** Merge all paths if they have common nodes */
  val MCCFilePath = outputDir + System.getProperty("file.separator") + "MCCPaths.txt"

  FileUtils.writeIterableToHDFS(MCCFilePath, MCCs)

  /**
   * Output RDD DAG to logger
   */
  logger.info(edgeListRDD.toDebugString + "\n")

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
