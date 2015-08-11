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
package org.dia

import java.io._
import java.text.SimpleDateFormat
import java.util.{Calendar, Random}

import org.dia.Constants._
import org.dia.TRMMUtils.Parsers
import org.dia.core.{SciSparkContext, sciTensor}
import org.dia.sLib.mccOps
import org.dia.tensors.AbstractTensor
import org.json4s.JsonAST.JObject
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.language.implicitConversions

/**
  */
object MainBreeze {

  /**
   * NetCDF variables to use
   * TODO:: Make the netcdf variables global - however this may need broadcasting
   */
  val rowDim = 180
  val columnDim = 360
  val TextFile = "TestLinks"


  /**
   * Used for reading/writing to database, files, etc.
   * Code From the book "Beginning Scala"
   * http://www.amazon.com/Beginning-Scala-David-Pollak/dp/1430219890
   */
  def using[A <: {def close(): Unit}, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def writeToFile(fileName:String, data:String) =
    using (new FileWriter(fileName)) {
      fileWriter => fileWriter.write(data)
    }

  def appendToFile(fileName:String, textData:String) =
    using (new FileWriter(fileName, true)){
      fileWriter => using (new PrintWriter(fileWriter)) {
        printWriter => printWriter.println(textData)
      }
    }

  def checkCriteria(origData: Array[Double], compData: Array[Double], compNum: Int): Boolean = {
    var result = false
    var area = 0.0;
    var maskedTen = compData.map(e => {
      if (e == compNum) {
        area += 1.0;
        1.0
      } else 0.0
    })
    var idx = 0
    var dMax = Double.MinValue
    var dMin = Double.MaxValue
    var origMasked = origData.map(e => {
      if (dMax < e && maskedTen(idx) != 0) dMax = e;
      if (dMin > e && maskedTen(idx) != 0) dMin = e;
      val nval = e * maskedTen(idx);
      idx += 1;
      nval
    })

    //    println("===============================" + compNum + "==" + area)
    //    println(DenseMatrix.create(20, 20, maskedTen))
    //    println("|||||||||||||||||||||||||||||||dMax:" + dMax + "||||dMin:" + dMin)
    //    println(DenseMatrix.create(20, 20, origMasked))
    //    println("|||||||||||||||||||||||||||||||")
    //    println(DenseMatrix.create(20, 20, origData))
    //    println("===============================")

    if ((area >= 40.0) || (area < 40.0) && ((dMax - dMin) > 10.0))
      result = true
    result
  }

  def overlap(origData: AbstractTensor, origData2: AbstractTensor, compNum1: Int, compNum2: Int): Boolean = {
    // mask for specific component
    var maskedData1 = origData.map(e => {
      if (e == compNum1) 1.0 else 0.0
    })
    var maskedData2 = origData2.map(e => {
      if (e == compNum2) 1.0 else 0.0
    })
    var result = false
    // check overlap
    ((maskedData1 * maskedData2).isZero == false)
  }

  def checkComponentsOverlap(sciTensor1: sciTensor, sciTensor2: sciTensor): List[(String, String)] = {
    val currentTimeRDD = mccOps.findCloudElementsX(sciTensor1)
    val nextTimeRDD = mccOps.findCloudElementsX(sciTensor2)
    var edgePair = List.empty[(String, String)]
    // cartesian product
    (1 to currentTimeRDD.metaData("NUM_COMPONENTS").toInt).map(cIdx => {
      (1 to nextTimeRDD.metaData("NUM_COMPONENTS").toInt).map(nIdx => {
        // check if valid
        if (checkCriteria(sciTensor1.tensor.data, currentTimeRDD.tensor.data, cIdx)
          && checkCriteria(sciTensor2.tensor.data, nextTimeRDD.tensor.data, nIdx)) {
          //verify overlap
          if (overlap(currentTimeRDD.tensor, nextTimeRDD.tensor, cIdx, nIdx)) {
            val tup = (currentTimeRDD.metaData("FRAME") + ":" + cIdx, nextTimeRDD.metaData("FRAME") + ":" + nIdx)
            edgePair :+= (tup)
          }
          // else don't do anything
        }
      })
    })
    edgePair
  }

  def main(args: Array[String]): Unit = {
    var master = "";
    var testFile = if (args.isEmpty) "TestLinks" else args(0)
    if (args.isEmpty || args.length <= 1) master = "local[50]" else master = args(1)

    val sc = new SciSparkContext(master, "test")

    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)
    //TotCldLiqH2O_A
    val variable = if (args.isEmpty || args.length <= 2) "TotCldLiqH2O_A" else args(2)

    val sRDD = sc.NetcdfFile(testFile, List(variable))

    //    val preCollected = sRDD.map(p => p(variable).reduceResolution(5))
    val preCollected = sRDD
    val filtered = preCollected.map(p => p(variable) <= 10000.0)

    //    val filCollected = filtered.collect()
    val filCartesian = filtered.cartesian(filtered)
      .filter(pair => {
      val d1 = Parsers.ParseDateFromString(pair._1.metaData("FRAME"))
      val d2 = Parsers.ParseDateFromString(pair._2.metaData("FRAME"))
      val sdf = new SimpleDateFormat("yyyy-MM-dd");
      val c = Calendar.getInstance();
      c.setTime(d1)
      c.add(Calendar.DATE, 1)
      val c2 = Calendar.getInstance()
      c2.setTime(d2)
      //      println("==================== " + sdf.format(c.getTime) + " " + sdf.format(c2.getTime))
      //      println(sdf.format(c.getTime).equals(sdf.format(c2.getTime)))
      //      println("====================")
      (!sdf.format(d1).equals(sdf.format(d2)) && !sdf.format(c.getTime).equals(sdf.format(c2.getTime)))
    }
      )
    //
    val edgesRdd = filCartesian.map(pair => {
      checkComponentsOverlap(pair._1, pair._2)
    })
    println("====================")
    println(edgesRdd.toDebugString)
    println("====================")
    val colEdges = edgesRdd.collect()
    var jsonNodes = scala.collection.mutable.Set[JObject]()
    var nodes = scala.collection.mutable.Set[String]()
    var edges = scala.collection.mutable.Set[JObject]()
    val generator = new Random()
    var totEdges = 0

    colEdges.map(edgesList => {
      if (edgesList.size > 0) {
        (0 to edgesList.size - 1).map(cnt => {
          var x = 0.0
          var y = 0.0
          if (generator.nextBoolean()) {
            x = generator.nextDouble % 1
            y = Math.sqrt(1-x*x)
          } else {
            y = generator.nextDouble % 1
            x = Math.sqrt(1-y*y)
          }
          if (!nodes.contains(edgesList(cnt)._1)) {
            nodes += edgesList(cnt)._1
            val color = "rgb(255," +generator.nextInt(256).toString+ ",102)"
            jsonNodes += ("id" -> edgesList(cnt)._1) ~ ("label" -> edgesList(cnt)._1) ~ ("size" -> 100) ~ ("x" -> x * 100.0) ~ ("y" -> y * 100.0) ~ ("color" -> color)
          }
          if (!nodes.contains(edgesList(cnt)._2)) {
            nodes += edgesList(cnt)._2
            val color = "rgb(255," +generator.nextInt(256).toString+ ",102)"
            jsonNodes += ("id" -> edgesList(cnt)._2) ~ ("label" -> edgesList(cnt)._2) ~ ("size" -> 100) ~ ("x" -> x * -100.0) ~ ("y" -> y * -100.0) ~ ("color" -> color)
          }
          edges += ("id" -> totEdges) ~ ("source" -> edgesList(cnt)._1) ~ ("target" -> edgesList(cnt)._2)
          totEdges += 1
        })
      }
    })
    val json = ("nodes" -> jsonNodes) ~ ("edges" -> edges)


    val data = Array("Five","strings","in","a","file!")
    writeToFile("/Users/marroqui/Downloads/sigma.js-master/examples/data/graph.json", pretty(render(json)))
  }
}


