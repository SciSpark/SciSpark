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

package org.dia.algorithms.mcs

import scala.collection.mutable

import org.apache.spark.rdd.RDD

import org.dia.core.{SciDataset, SciSparkContext, SRDDFunctions}
import org.dia.tensors.AbstractTensor
import org.dia.utils.FileUtils

/**
 * Runs Grab em' Tag em' Graph em'
 * Data is taken from local file system or HDFS through
 * Spark's experimental "sc.binaryFiles".
 *
 * The algorithm assumes that the variable arrays
 * "longitude" and "latitude" exist in the Netcdf Files
 * at the given path.
 */
class GTGRunner(
    val masterURL: String,
    val paths: String,
    val varName: String,
    val partitions: Int,
    val outputDir: String,
    val maxAreaOverlapThreshold: Double = 0.65,
    val minAreaOverlapThreshold: Double = 0.50,
    val outerTemp: Double = 241.0,
    val innerTemp: Double = 233.0,
    val convectiveFraction: Double = 0.9,
    val minArea: Int = 625,
    val nodeMinArea: Int = 150,
    val minAreaThres: Int = 16,
    val minGraphLength: Int = 4) {

  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def run(): Unit = {

    logger.info("Starting MCS")
    val outputDir = FileUtils.checkHDFSWrite(this.outputDir)
    /**
     * Initialize the spark context to point to the master URL
     */
    val sc = new SciSparkContext(masterURL, "DGTG : Distributed MCS Search")

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
    val sRDD = sc.sciDatasets(paths, List(varName, "longitude", "latitude"), partitions)

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
    // val consecFrames = pairConsecutiveFrames(filtered, "FRAME")

    /**
     * Core MCS
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
     * Collect the edgeList and construct NodeMap
     */
    val MCSEdgeList = edgeListRDD.collect()
    val MCSNodeMap = MCSOps.createNodeMapFromEdgeList(MCSEdgeList, lat, lon)

    val broadcastedNodeMap = sc.sparkContext.broadcast(MCSNodeMap)

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
     * Generate the netcdfs
     */
    edgeListRDD.foreach(edge => {
      val nodeMap = broadcastedNodeMap.value
      MCSUtils.writeEdgeNodesToNetCDF(edge, nodeMap, lat, lon, true, "/tmp", null)
    })

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
     * Output RDD DAG to logger
     */
    logger.info(edgeListRDD.toDebugString + "\n")
  }

}

