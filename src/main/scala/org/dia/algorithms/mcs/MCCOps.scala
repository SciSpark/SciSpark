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

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object MCCOps {

  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * To find MCC feature from the subgraphs (cloud clusters) found
   * @param subgraph Subgraph generated from the previous MCC step
   * @param minFeatureLength Min length of the MCC to be found
   * @param maxFeatureLength Max length of contunious nodes to consider in an MCC
   * @param potentialNodeSet A Set of nodes that match Criteria A and B in Laurent et al 1998
   */
  def findMCC(
      subgraph: mutable.Iterable[(String, String)],
      minFeatureLength: Int,
      maxFeatureLength: Int,
      potentialNodeSet: Broadcast[mutable.HashSet[String]]):
      mutable.MutableList[mutable.HashSet[String]] = {

    // Potential set of nodes that match the criteria
    // val potentialNodes = new mutable.HashSet[String]()
    // A map of a node to all its outgoing nodes
    val outgoingEdgeMap = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]

    // A set of source nodes
    val srcNodeSet = new mutable.HashSet[String]()

    /** Create the outgoung edge map and src node set */
    for(edge <- subgraph) {
      outgoingEdgeMap.addBinding(edge._1, edge._2)
      srcNodeSet.add(edge._1)
      if (srcNodeSet.contains(edge._2)) {
        srcNodeSet.remove(edge._2)
      }
    }

    /** Map to hold all potential MCC paths */
    val globalMCCs = new mutable.HashMap[String, mutable.MutableList[mutable.HashSet[String]]]()
    val visitedNodeSet = new mutable.HashSet[String]()

    for (node <- srcNodeSet.toList.sorted) {
      val MCCNodes = traversePath (node, outgoingEdgeMap,
        globalMCCs, potentialNodeSet.value, visitedNodeSet)
    }

    /** Collapse all potential paths into one map keyed by source node and filter by min feature length */
    val collapsedPotentialPaths = globalMCCs.map({
      case (src, paths) =>
        val tempSet = new mutable.HashSet[String]()
        paths.foreach(tempSet ++= _)
        (src, tempSet)
    })
    val result = mutable.MutableList[mutable.HashSet[String]]()
    globalMCCs.foreach({
      case (src, paths) =>
        paths.foreach({
          case path =>
            if (path.size >= minFeatureLength && path.size <= maxFeatureLength) {
              result += path
            }
        })
    })

    /** Find any other paths that may connect to the qualifying MCCs */
    val mergedMCCs = new mutable.MutableList[mutable.HashSet[String]]()
    result.foreach({
      case path =>
        var merged = false
        for (p <- mergedMCCs) {
          if ((p.intersect(path)).size != 0) {
            p ++= path
            merged = true
          }
        }
        if(!merged) {
          mergedMCCs += path
        }
    })
    collapsedPotentialPaths.foreach({
      case (src, path) =>
        for (p <- mergedMCCs) {
          if ((p.intersect(path)).size != 0) {
            p ++= path
          }
        }
    })

    return mergedMCCs
    }

  /**
   * To traverse subgraph from a node to find MCCs
   * @param node Node to start traversal from
   * @param outgoingEdgeMap Map of all nodes with its corresponding outgoing connections
   * @param globalMCCs Map of potential MCC paths found while traversing the graph
   * @param potentialNodeSet A set of all nodes matching the Laurent et al 1998 criteria
   * @param visitedNodeSet Set of visited nodes
   * @return
   */
  def traversePath(node: String,
      outgoingEdgeMap: mutable.HashMap[String, mutable.Set[String]],
      globalMCCs: mutable.HashMap[String, mutable.MutableList[mutable.HashSet[String]]],
      potentialNodeSet: mutable.HashSet[String],
      visitedNodeSet: mutable.HashSet[String]): mutable.MutableList[mutable.HashSet[String]] = {

    if (visitedNodeSet.contains(node)) {
      if (globalMCCs.contains(node)) {
        return globalMCCs(node)
      }
      else return new mutable.MutableList[mutable.HashSet[String]]()
    }

    val isPotentialMCCNode = potentialNodeSet.contains(node)
    visitedNodeSet.add(node)

    if (outgoingEdgeMap.contains(node)) {
      for (childNode <- outgoingEdgeMap(node)) {
        val MCCNodeSet = traversePath(childNode, outgoingEdgeMap, globalMCCs,
          potentialNodeSet, visitedNodeSet)
        /** If current node is a potential node then add it to the paths returned bu its children */
        if (isPotentialMCCNode) {
          for (path <- MCCNodeSet) {
            if (path.size > 0) {
              val tempSet = new mutable.HashSet[String]()
              tempSet ++= path
              tempSet += node
              if (globalMCCs.contains(node)) {
                globalMCCs(node) += tempSet
              }
              else {
                val tempList = new mutable.MutableList[mutable.HashSet[String]]()
                tempList += tempSet
                globalMCCs(node) = tempList
              }
            }
          }
        }
      }
    }
    if (isPotentialMCCNode) {
      if (!globalMCCs.contains(node)) {
        val path = new mutable.HashSet[String]()
        path += node
        val tempList = new mutable.MutableList[mutable.HashSet[String]]()
        tempList += path
        globalMCCs(node) = tempList
      }
      return globalMCCs(node)
    }
    else return new mutable.MutableList[mutable.HashSet[String]]()
  }

  /**
   * Check if a given node satisfies the two criterions specified by Laurent et al 1998
   * @param node The MCS Node to check
   * @param areaCriteriaA
   * @param areaCriteriaB
   * @param tempCriteriaA
   * @param tempCriteriaB
   * @return A tuple with a boolean indicating if the node satisfies the criteria or not
   */
  def isPotentialNode(node : MCSNode,
      areaCriteriaA: Int,
      areaCriteriaB: Int,
      tempCriteriaA: Float,
      tempCriteriaB: Float):
      (Boolean, String) = {

    /** Area within the node matching the temp criteria for A acc to Laurent et al 1998 */
    var nodeAreaCriteriaA: Int = 0
    /** Area within the node matching the temp criteria for B acc Laurent et al 1998 */
    var nodeAreaCriteriaB: Int = 0

    for ((pos, temp) <- node.grid) {
      // Acc to Laurent et al temp for criteria ! is less than criteria B
      if (temp <= tempCriteriaA) {
        nodeAreaCriteriaA += 1
        nodeAreaCriteriaB += 1
      }
      else if (temp <= tempCriteriaB) {
        nodeAreaCriteriaB += 1
      }
    }
    if (nodeAreaCriteriaA >= areaCriteriaA && nodeAreaCriteriaB >= areaCriteriaB) {
      return (true, node.hashKey())
    }
    else {
      return (false, node.hashKey())
    }
  }

  /**
   * Load the subgraphs found in the previous steps from the files spcified in filepath
   * @param filepath
   * @param sc
   * @return
   */
  def loadSubgraphsFromFile(filepath: String, sc: SparkContext):
  RDD[mutable.MutableList[(String, String)]] = {
    val textfiles = sc.textFile(filepath).map(x => {
      val edgesString = x.replace("Set(", "")
        .replace(")", "")
        .split(", ")
      val edgesTupleList = new mutable.MutableList[(String, String)]
      for (edge <- edgesString) {
        val nodes = edge.split(",")
        edgesTupleList += ((nodes(0), nodes(1)))
      }
      edgesTupleList
    })
    return textfiles
  }
}
