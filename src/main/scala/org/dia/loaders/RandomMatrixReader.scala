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

import org.dia.core.SciDataset
import org.dia.tensors.Nd4jTensor

/**
 * Generates random matrices.
 */
object RandomMatrixReader {

  def dist(x: Double, y: Double): Double = Math.pow(x - y, 2)

  def loadRandomArray(uri: String, varname: String): (Array[Double], Array[Int]) = {
    val random = new Random()
    random.setSeed(uri.hashCode)
    val n = 20
    val center1 = random.nextDouble * n
    val center2 = random.nextDouble * n
    val center3 = random.nextDouble * n
    val ndArray = Nd4j.zeros(n, n)
    for (row <- 0 to n - 1) {
      for (col <- 0 to n - 1) {
        if (dist(row, center1) + dist(col, center1) <= 9) ndArray.put(row, col, random.nextDouble * 340)
        if (dist(row, center2) + dist(col, center2) <= 9) ndArray.put(row, col, random.nextDouble * 7000)
        if (dist(row, center3) + dist(col, center3) <= 9) ndArray.put(row, col, random.nextDouble * 24000)
      }
    }
    (ndArray.data.asDouble, ndArray.shape)
  }

  def loadRandomArray(sizeTuple: (Int, Int))(uri: String, varname: String): (Array[Double], Array[Int]) = {
    val random = new Random()
    random.setSeed(uri.hashCode)
    val m = sizeTuple._1
    val n = sizeTuple._2
    val center1Row = random.nextDouble * m
    val center1Col = random.nextDouble * n
    val center2Row = random.nextDouble * m
    val center2Col = random.nextDouble * n
    val center3Row = random.nextDouble * m
    val center3Col = random.nextDouble * n
    val ndArray = Nd4j.zeros(m, n)
    for (row <- 0 to m - 1) {
      for (col <- 0 to n - 1) {
        if (dist(row, center1Row) + dist(col, center1Col) <= 9) ndArray.put(row, col, random.nextDouble * 340)
        if (dist(row, center2Row) + dist(col, center2Col) <= 9) ndArray.put(row, col, random.nextDouble * 7000)
        if (dist(row, center3Row) + dist(col, center3Col) <= 9) ndArray.put(row, col, random.nextDouble * 24000)
      }
    }
    (ndArray.data.asDouble, ndArray.shape)
  }


  def createRandomSciDataset(name: String,
                             variables : List[(String, Array[Int])],
                             attributes : List[(String, String)]): SciDataset = {
    val SciVars = variables.map({
      case (filename, shape) =>
        val nd4jTensor = new Nd4jTensor(Nd4j.rand(shape, filename.hashCode.toLong)) * 300.0
        val dims = shape.zip(Array("u", "v", "w", "x", "y", "z"))
          .map({ case (dim, name) => (filename + "_" + name.toString, dim) }).toList
        (filename, new org.dia.core.Variable(filename, nd4jTensor, dims))
    })

    new org.dia.core.SciDataset(SciVars, attributes, name)
  }

}
