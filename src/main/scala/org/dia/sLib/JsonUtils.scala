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
package org.dia.sLib

import java.util.Random

import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._

import scala.collection.mutable

/**
 * JSON utils
 */
object JsonUtils {

  def generateJson(edgesList: List[(String, String)],
                   dates: mutable.HashMap[String, Int],
                   nodes: Set[String]): (mutable.Set[JObject], mutable.Set[JObject]) = {
    var totEdges = 0
    val generator = new Random()
    var xPos = generator.nextDouble % 1
    var yPos = 0.0

    var jsonNodes = mutable.Set[JObject]()
    var jsonEdges = mutable.Set[JObject]()
    var setNodes = scala.collection.mutable.Set[String]()
    val dd = dates.map(entry => (entry._2, entry._1))

    edgesList.indices.foreach(cnt => {
      var x = generator.nextDouble % 1
      var y = 0.0
      if (!setNodes.contains(edgesList(cnt)._1)) {
        setNodes += edgesList(cnt)._1
        val color = "rgb(255," + generator.nextInt(256).toString + ",102)"

        jsonNodes += ("id" -> edgesList(cnt)._1) ~ ("label" -> dd.get(edgesList(cnt)._1.toInt)) ~ ("size" -> 100) ~ ("x" -> (xPos+100.0)) ~ ("y" -> (dd.get(edgesList(cnt)._1.toInt).hashCode() % 3 +100)) ~ ("color" -> color)

      }
      if (!setNodes.contains(edgesList(cnt)._2)) {
        setNodes += edgesList(cnt)._2
        val color = "rgb(255," + generator.nextInt(256).toString + ",102)"
        jsonNodes += ("id" -> edgesList(cnt)._2) ~ ("label" -> dd.get(edgesList(cnt)._2.toInt)) ~ ("size" -> 100) ~ ("x" -> (xPos+200.0)) ~ ("y" -> (dd.get(edgesList(cnt)._1.toInt).hashCode() % 3 +10000)) ~ ("color" -> color)
      }
      jsonEdges += ("id" -> totEdges) ~ ("source" -> edgesList(cnt)._1) ~ ("target" -> edgesList(cnt)._2)
      xPos+= 500.0
      totEdges += 1

    })
    (jsonNodes, jsonEdges)
  }
}
