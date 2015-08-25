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

import org.dia.Parsers
import org.joda.time.{DateTime, Days, MutableDateTime}
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._

import scala.collection.mutable

/**
 * JSON utils
 */
object JsonUtils {

  def generateJson(edgesList:  Array[((String, String), (String, String))],
                   dates: mutable.HashMap[String, Int],
                   nodes:  Set[(String, String)]): (mutable.Set[JObject], mutable.Set[JObject]) = {
    var totEdges = 0
    val generator = new Random()
    var xPos = generator.nextDouble % 1
    var yPos = 0.0

    var jsonNodes = mutable.Set[JObject]()
    var jsonEdges = mutable.Set[JObject]()
    var setNodes = scala.collection.mutable.Set[(String, String)]()
    val dd = dates.map(entry => (entry._2, entry._1))//Parsers.ParseDateFromString(entry._1).getTime)

    val epoch = new MutableDateTime
    epoch.setDate(0); //Set to Epoch time
    val now = new DateTime

//    val days = Days.daysBetween(epoch, now);

    edgesList.indices.foreach(cnt => {
      var x = generator.nextDouble % 1
      var y = 0.0
      val days = Days.daysBetween(new MutableDateTime(Parsers.ParseDateFromString(dd.get(edgesList(cnt)._1._1.toInt).get).getTime), now)
      if (!setNodes.contains(edgesList(cnt)._1)) {
        setNodes += edgesList(cnt)._1
        val color = "rgb(255," + generator.nextInt(256).toString + ",102)"

        jsonNodes += ("id" -> edgesList(cnt)._1.toString) ~ ("label" -> (dd.get(edgesList(cnt)._1._1.toInt).get+":"+edgesList(cnt)._1._2)) ~ ("size" -> 100) ~ ("x" -> (xPos+100.0)) ~ ("y" -> days.getDays) ~ ("color" -> color)

      }
      val days2 = Days.daysBetween(new MutableDateTime(Parsers.ParseDateFromString(dd.get(edgesList(cnt)._2._1.toInt).get).getTime), now)
      if (!setNodes.contains(edgesList(cnt)._2)) {
        setNodes += edgesList(cnt)._2
        val color = "rgb(255," + generator.nextInt(256).toString + ",102)"

        jsonNodes += ("id" -> edgesList(cnt)._2.toString) ~ ("label" -> (dd.get(edgesList(cnt)._2._1.toInt).get+":"+edgesList(cnt)._2._2)) ~ ("size" -> 100) ~ ("x" -> (xPos+200.0)) ~ ("y" -> days2.getDays) ~ ("color" -> color)
      }
      jsonEdges += ("id" -> totEdges) ~ ("source" -> edgesList(cnt)._1.toString) ~ ("target" -> edgesList(cnt)._2.toString)
      xPos+= 500.0
      totEdges += 1

    })
    (jsonNodes, jsonEdges)
  }
}
