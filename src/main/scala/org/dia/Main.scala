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

import org.dia.Constants._
import org.dia.core.{SciSparkContext, sciTensor}
import org.dia.sLib.mccOps
import org.dia.tensors.Nd4jTensor
import org.nd4j.api.Implicits._
import org.nd4j.linalg.factory.Nd4j

import scala.language.implicitConversions

/**
  */
object Main {

  /**
   * NetCDF variables to use
   * TODO:: Make the netcdf variables global - however this may need broadcasting
   */
  val rowDim = 180
  val columnDim = 360
  val TextFile = "TestLinks"


  def main(args: Array[String]): Unit = {
    var master = "";
    var testFile = if (args.isEmpty) "TestLinks" else args(0)
    if(args.isEmpty || args.length <= 1) master = "local[4]" else master = args(1)

    val sc = new SciSparkContext(master, "test")

    sc.setLocalProperty(ARRAY_LIB, ND4J_LIB)

    val variable = if(args.isEmpty || args.length <= 2) "TotCldLiqH2O_A" else args(2)

    val sRDD = sc.NetcdfFile(testFile, List(variable))

    val preCollected = sRDD.map(p => p(variable).reduceResolution(5))

    val filtered = preCollected.map(p => p(variable) <= 0.0009)

    val componentFrameRDD = filtered.flatMap(p => mccOps.findConnectedComponents(p))

    val criteriaRDD = componentFrameRDD.filter(p => {
      val hash = p.metaData
      val area = hash("AREA").toDouble
      val tempDiff = hash("DIFFERENCE").toDouble
      (area > 40.0) || (tempDiff < 10.0)
    })


    //val Sliced = filtered.map(p => p(variable)(4 -> 9, 2 -> 5))

    val collected: Array[sciTensor] = criteriaRDD.collect

    collected.map(p => {
      println(p.metaData("AREA"))
      println(p.metaData("DIFFERENCE"))
      println(p.varInUse)
      println(p.tensor)
    })





  }
}


