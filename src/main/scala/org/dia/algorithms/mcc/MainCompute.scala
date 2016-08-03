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

import scala.language.implicitConversions

import org.dia.Constants._
import org.dia.core.SciSparkContext

/**
 * Implements MCC with Cartesian + [neither Cartesian nor in-place approach].
 * Data is random matrices.
 */
object MainCompute {

  /**
   * NetCDF variables to use
   * @todo Make the NetCDF variables global - however this may need broadcasting
   */
  val rowDim = 180
  val columnDim = 360
  val TextFile = "TestLinks"
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val master = if (args.isEmpty || args.length <= 1) "local[50]" else args(1)
    val testFile = if (args.isEmpty) TextFile else args(0)

    val sc = new SciSparkContext(master, "test")
    logger.info("SciSparkContext created")

    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)

    val partitionNum = if (args.isEmpty || args.length <= 2) 2 else args(2).toInt
    val variable = if (args.isEmpty || args.length <= 3) "TotCldLiqH2O_A" else args(3)
    val dimension = if (args.isEmpty || args.length <= 4) (20, 20) else (args(4).toInt, args(4).toInt)
    val sRDD = sc.randomMatrices(testFile, List(variable), dimension, partitionNum)

    val filtered = sRDD.map(p => p(variable) <= 241.0)
    logger.info("Matrices have been filtered")

    val consecFrames = filtered.cartesian(filtered)
      .filter({
        case (t1, t2) =>
          val d1 = Integer.parseInt(t1.metaData("FRAME"))
          val d2 = Integer.parseInt(t2.metaData("FRAME"))
          (d1 + 1) == d2
      })
    logger.info("Pairing consecutive frames is done.")

    val edgesRdd = consecFrames.map({
      case (t1, t2) => MCCOps.checkComponentsOverlap(t1, t2)
    })
    logger.info("Checked edges and overlap.")

    val collectedEdges = edgesRdd.collect()

  }

}
