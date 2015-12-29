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

import org.dia.Constants._
import org.dia.core.{ SciSparkContext, SRDD, SciTensor }
import org.slf4j.Logger
import scala.collection.mutable
import scala.language.implicitConversions

/**
 * Implements MCC with Cartesian product + Cartesian product.
 * Source is random matrices.
 */
object MainMemory {

  /**
   * NetCDF variables to use
   * @todo Make the NetCDF variables global - however this may need broadcasting
   */
  val rowDim = 20
  val colDim = 20
  val textFile = "TestLinks"
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val master = if (args.isEmpty || args.length <= 1) "local[24]" else args(1)
    val testFile = if (args.isEmpty) textFile else args(0)

    val sc = new SciSparkContext(master, "test")
    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)

    val partitionNum = if (args.isEmpty || args.length <= 2) 2 else args(2).toInt
    val variable = if (args.isEmpty || args.length <= 3) "TotCldLiqH2O_A" else args(3)
    val dimension = if (args.isEmpty || args.length <= 4) (rowDim, colDim) else (args(4).toInt, args(4).toInt)
    val sRDD = sc.randomMatrices(testFile, List(variable), dimension, partitionNum)

    val filtered = sRDD.map(p => p(variable) <= 241.0)
    logger.info("Matrices have been filtered")

    val consecFrames = filtered.cartesian(filtered).
      filter({ case (t1, t2) => t1.metaData("FRAME").toInt == t2.metaData("FRAME").toInt - 1 })

    val componentFrameRDD = consecFrames.flatMap({
      case (t1, t2) => {
        val comps1 = MCCOps.findCloudComponents(t1).filter(MCCOps.checkCriteria)
        val comps2 = MCCOps.findCloudComponents(t2).filter(MCCOps.checkCriteria)
        val compsPairs = for (x <- comps1; y <- comps2) yield (x, y)
        val overlaps = compsPairs.filter({ case (t1, t2) => !(t1.tensor * t2.tensor).isZeroShortcut })
        overlaps.map({ case (t1, t2) => ((t1.metaData("FRAME"), t1.metaData("COMPONENT")), (t2.metaData("FRAME"), t2.metaData("COMPONENT"))) })
      }
    })

    val collectedEdges = componentFrameRDD.collect()
    val collectedVertices = collectedEdges.flatMap({ case (n1, n2) => List(n1, n2) }).toSet
    println(collectedVertices.toList.sortBy(_._1))
    println(collectedEdges.toList.sorted)
    println(collectedVertices.size)
    println(collectedEdges.length)
  }

}


