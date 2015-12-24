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
package org.dia.loaders

import java.util.Random
import org.nd4j.linalg.factory.Nd4j

/**
 * Generates random matrices.
 */
object RandomMatrixReader {

  def dist(x: Double, y: Double) = Math.pow(x - y, 2)

  def loadRandomArray(uri: String, varname: String): (Array[Double], Array[Int]) = {
    val generator = new Random()
    generator.setSeed(uri.hashCode)
    val n = 20
    val randomCenter1 = generator.nextDouble * n
    val randomCenter2 = generator.nextDouble * n
    val randomCenter3 = generator.nextDouble * n
    val ndArray = Nd4j.zeros(n, n)
    for (row <- 0 to n - 1) {
      for (col <- 0 to n - 1) {
        if (dist(row, randomCenter1) + dist(col, randomCenter1) <= 9) ndArray.put(row, col, generator.nextDouble * 340)
        if (dist(row, randomCenter2) + dist(col, randomCenter2) <= 9) ndArray.put(row, col, generator.nextDouble * 7000)
        if (dist(row, randomCenter3) + dist(col, randomCenter3) <= 9) ndArray.put(row, col, generator.nextDouble * 24000)
      }
    }
    (ndArray.data.asDouble, ndArray.shape)
  }

  def loadRandomArray(sizeTuple: (Int, Int))(uri: String, varname: String): (Array[Double], Array[Int]) = {
    val generator = new Random()
    generator.setSeed(uri.hashCode)
    val m = sizeTuple._1
    val n = sizeTuple._2
    val randomCenter1Row = generator.nextDouble * m
    val randomCenter1Col = generator.nextDouble * n
    val randomCenter2Row = generator.nextDouble * m
    val randomCenter2Col = generator.nextDouble * n
    val randomCenter3Row = generator.nextDouble * m
    val randomCenter3Col = generator.nextDouble * n
    val ndArray = Nd4j.zeros(m, n)
    for (row <- 0 to m - 1) {
      for (col <- 0 to n - 1) {
        if (dist(row, randomCenter1Row) + dist(col, randomCenter1Col) <= 9) ndArray.put(row, col, generator.nextDouble * 340)
        if (dist(row, randomCenter2Row) + dist(col, randomCenter2Col) <= 9) ndArray.put(row, col, generator.nextDouble * 7000)
        if (dist(row, randomCenter3Row) + dist(col, randomCenter3Col) <= 9) ndArray.put(row, col, generator.nextDouble * 24000)
      }
    }
    (ndArray.data.asDouble, ndArray.shape)
  }

}
