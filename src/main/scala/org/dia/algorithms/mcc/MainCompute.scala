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

import java.util.Random
import org.dia.Constants._
import org.dia.core.{ SciSparkContext, SciTensor }
import org.dia.tensors.AbstractTensor
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.slf4j.Logger
import scala.collection.mutable
import scala.language.implicitConversions

/**
 */
object MainCompute {

  /**
   * NetCDF variables to use
   * TODO:: Make the netcdf variables global - however this may need broadcasting
   */
  val rowDim = 180
  val columnDim = 360
  val TextFile = "TestLinks"
  // Class logger
  val LOG: Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  var nodes = scala.collection.mutable.Set[String]()
  var totEdges = 0

  def main(args: Array[String]): Unit = {
    var master = ""
    val testFile = if (args.isEmpty) TextFile else args(0)
    if (args.isEmpty || args.length <= 1) master = "local[50]" else master = args(1)

    val sc = new SciSparkContext(master, "test")
    LOG.info("SciSparkContext created")

    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)
    //TotCldLiqH2O_A
    val partitionNum = if (args.isEmpty || args.length <= 2) 2 else args(2).toInt
    val variable = if (args.isEmpty || args.length <= 3) "TotCldLiqH2O_A" else args(3)
    val dimension = if (args.isEmpty || args.length <= 4) (20, 20) else (args(4).toInt, args(4).toInt)
    val netcdfFile = sc.randomMatrices(testFile, List(variable), dimension, partitionNum)
    val sRDD = netcdfFile

    val preCollected = sRDD
    val filtered = preCollected.map(p => p(variable) <= 241.0)
    LOG.info("Matrices have been filtered")

    val filCartesian = filtered.cartesian(filtered)
      .filter(pair => {
        val d1 = Integer.parseInt(pair._1.metaData("FRAME"))
        val d2 = Integer.parseInt(pair._2.metaData("FRAME"))
        (d1 + 1) == d2
      })
    LOG.info("C" +
      "" +
      "" +
      "artesian product have been done.")

    val edgesRdd = filCartesian.map(pair => {
      checkComponentsOverlap(pair._1, pair._2)
    })
    LOG.info("Checked edges and overlap.")

    val colEdges = edgesRdd.collect()

    var jsonNodes = mutable.Set[JObject]()
    var jsonEdges = mutable.Set[JObject]()

    //    colEdges.map(edgesList => {
    //      if (edgesList.nonEmpty) {
    //        val res = generateJson(edgesList, dates)
    //        jsonNodes ++= res._1
    //        jsonEdges ++= res._2
    //      }
    //    })
    println("*****************")
    println(nodes.size)
    println(totEdges)
    println("*****************")
    //    val json = ("nodes" -> jsonNodes) ~ ("edges" -> jsonEdges)
    //    FileUtils.writeToFile("./resources/graph.json", pretty(render(json)))
  }

  def checkComponentsOverlap(sciTensor1: SciTensor, sciTensor2: SciTensor): List[(String, String)] = {
    val currentTimeRDD = MCCOps.findCloudElements(sciTensor1)
    val nextTimeRDD = MCCOps.findCloudElements(sciTensor2)
    var edgePair = List.empty[(String, String)]
    // cartesian product
    (1 to currentTimeRDD.metaData("NUM_COMPONENTS").toInt).foreach(cIdx => {
      (1 to nextTimeRDD.metaData("NUM_COMPONENTS").toInt).foreach(nIdx => {
        // check if valid
        if (checkCriteria(sciTensor1.tensor.data, currentTimeRDD.tensor.data, cIdx)
          && checkCriteria(sciTensor2.tensor.data, nextTimeRDD.tensor.data, nIdx)) {
          //verify overlap
          if (overlap(currentTimeRDD.tensor, nextTimeRDD.tensor, cIdx, nIdx)) {
            val tup = (currentTimeRDD.metaData("FRAME") + ":" + cIdx, nextTimeRDD.metaData("FRAME") + ":" + nIdx)
            edgePair :+= tup
          }
          // else don't do anything
        }
      })
    })
    edgePair
  }

  def checkCriteria(origData: Array[Double], compData: Array[Double], compNum: Int): Boolean = {
    var result = false
    var area = 0.0
    val maskedTen = compData.map(e => {
      if (e == compNum) {
        area += 1.0
        1.0
      } else 0.0
    })
    var idx = 0
    var dMax = Double.MinValue
    var dMin = Double.MaxValue
    val origMasked = origData.map(e => {
      if (dMax < e && maskedTen(idx) != 0) dMax = e
      if (dMin > e && maskedTen(idx) != 0) dMin = e
      val nval = e * maskedTen(idx)
      idx += 1
      nval
    })

    if ((area >= 40.0) || (area < 40.0) && ((dMax - dMin) > 10.0))
      result = true
    result
  }

  def overlap(origData: AbstractTensor, origData2: AbstractTensor, compNum1: Int, compNum2: Int): Boolean = {
    // mask for specific component
    val maskedData1 = origData.map(e => {
      if (e == compNum1) 1.0 else 0.0
    })
    val maskedData2 = origData2.map(e => {
      if (e == compNum2) 1.0 else 0.0
    })
    var result = false
    // check overlap
    (maskedData1 * maskedData2).isZero == false
  }

  def generateJson(edgesList: List[(String, String)], dates: mutable.HashMap[Int, String]): (mutable.Set[JObject], mutable.Set[JObject]) = {
    val generator = new Random()
    var jsonNodes = mutable.Set[JObject]()
    var jsonEdges = mutable.Set[JObject]()

    edgesList.indices.foreach(cnt => {
      var x = 0.0
      var y = 0.0
      if (generator.nextBoolean()) {
        x = generator.nextDouble % 1
        y = Math.sqrt(1 - x * x)
      } else {
        y = generator.nextDouble % 1
        x = Math.sqrt(1 - y * y)
      }
      if (!nodes.contains(edgesList(cnt)._1)) {
        nodes += edgesList(cnt)._1
        val color = "rgb(255," + generator.nextInt(256).toString + ",102)"
        jsonNodes += ("id" -> edgesList(cnt)._1) ~ ("label" -> dates.get(edgesList(cnt)._1.split(":").apply(0).toInt)) ~ ("size" -> 100) ~ ("x" -> x * 100.0) ~ ("y" -> y * 100.0) ~ ("color" -> color)
      }
      if (!nodes.contains(edgesList(cnt)._2)) {
        nodes += edgesList(cnt)._2
        val color = "rgb(255," + generator.nextInt(256).toString + ",102)"
        jsonNodes += ("id" -> edgesList(cnt)._2) ~ ("label" -> dates.get(edgesList(cnt)._1.split(":").apply(0).toInt)) ~ ("size" -> 100) ~ ("x" -> x * -100.0) ~ ("y" -> y * -100.0) ~ ("color" -> color)
      }
      jsonEdges += ("id" -> totEdges) ~ ("source" -> edgesList(cnt)._1) ~ ("target" -> edgesList(cnt)._2)
      totEdges += 1
    })
    (jsonNodes, jsonEdges)
  }
}