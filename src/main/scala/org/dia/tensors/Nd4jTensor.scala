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
package org.dia.tensors

import org.dia.TRMMUtils.Constants._
import org.dia.TRMMUtils.NetCDFUtils
import org.nd4j.api.linalg.DSL._
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.factory.Nd4j
import ucar.nc2.dataset.NetcdfDataset

import scala.collection.mutable
import scala.language.implicitConversions

/**
 * The Nd4j Functional operations
 * Created by rahulsp on 7/6/15.
 */

class Nd4jTensor(t : (Any) => (Array[Double], Array[Int])) extends AbstractTensor {
  val tensor : INDArray = convert(t)
  type T = Nd4jTensor
  val name : String = "nd4j"

  def convert(loadFunc : (Any) => (Array[Double], Array[Int])) : INDArray {
    null
  }
  
  def reduceResolution(blockSize: Int): Nd4jTensor = {
    val largeArray = iNDArray
    val numRows = largeArray.rows()
    val numCols = largeArray.columns()

    val reducedSize = numRows * numCols / (blockSize * blockSize)

    val reducedMatrix = Nd4j.create(numRows / blockSize, numCols / blockSize)

    for(row <- 0 to reducedMatrix.rows - 1){
      for(col <- 0 to reducedMatrix.columns - 1){
        val rowRange = (row*blockSize to ((row + 1) * blockSize) - 1).toSet
        val columnRange = (col * blockSize to ((col + 1) * blockSize) - 1).toSet
        val crossProductRanges = for { x <- rowRange; y <- columnRange} yield (x, y)
        val block = crossProductRanges.map(pair => largeArray.getDouble(pair._1, pair._2))
        val numNonZero = block.count(p => p != 0)
        val avg = if (numNonZero > 0) (block.sum / numNonZero) else 0.0
        reducedMatrix.put(row, col, avg)
      }
    }
    new Nd4jTensor(reducedMatrix)
  }

  def +(array : Nd4jTensor) : Nd4jTensor = {
    new Nd4jTensor(iNDArray + array.iNDArray)
  }
}
