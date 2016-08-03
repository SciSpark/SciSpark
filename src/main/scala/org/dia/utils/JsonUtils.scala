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
package org.dia.utils

import scala.collection.mutable

import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._

/**
 * Utilities to write JSONs.
 */
object JsonUtils {

  /**
   * Generates JSON out of (MCC) graph.
   *
   * @param nodes Nodes of the (MCC) graph. A node is a tuple (frameID,componentID)
   * @param edges Edges of the (MCC) graph
   * @param dates Hash map of dates
   * @return Sets of JSON objects for nodes and edges which
   * can then be written to a file.
   */
  def generateJson(nodes: Set[(String, String)], edges: Array[((String, String), (String, String))],
    dates: mutable.HashMap[String, Int]): (mutable.Set[JObject], mutable.Set[JObject]) = {
    var totEdges = 0
    var jsonNodes = mutable.Set[JObject]()
    var jsonEdges = mutable.Set[JObject]()
    var setNodes = mutable.Set[(String, String)]()
    val dd = dates.map(entry => (entry._2, entry._1))

    edges.foreach(edge => {
      val sourceNode = edge._1
      if (!setNodes.contains(sourceNode)) {
        setNodes += sourceNode
        val (label, edgeString) = ("label" -> (dd.get(sourceNode._1.toInt).get + ":" + sourceNode._2))
        jsonNodes += ("id" -> sourceNode.toString) ~ (label, edgeString)
      }
      val targetNode = edge._2
      if (!setNodes.contains(targetNode)) {
        setNodes += targetNode
        val (label, edgeString) = ("label" -> (dd.get(targetNode._1.toInt).get + ":" + targetNode._2))
        jsonNodes += ("id" -> targetNode.toString) ~ (label, edgeString)
      }
      jsonEdges += ("id" -> totEdges) ~ ("source" -> sourceNode.toString) ~ ("target" -> targetNode.toString)
      totEdges += 1
    })
    (jsonNodes, jsonEdges)
  }

}
