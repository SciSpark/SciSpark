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
package org.dia.core

import breeze.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.dia.Constants._
import org.dia.tensors.{AbstractTensor, BreezeTensor, Nd4jTensor}
import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite

/**
 * Mesoscale convective complex (MCC) test
 */
class MCCAlgorithmTest extends FunSuite {

  test("mappedReduceResolutionTest") {
    val sc = SparkTestConstants.sc
    sc.setLocalProperty(ARRAY_LIB, ND4J_LIB)
    val variable = "TotCldLiqH2O_A"
    val nd4jRDD = sc.OpenDapURLFile("TestLinks2", variable)
    val preCollected  = nd4jRDD.map(p => p(variable).reduceResolution(5))
    val collected: Array[sciTensor] = preCollected.collect

    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)
    val breezeRDD = sc.OpenDapURLFile("TestLinks2", variable)
    val breezeCollect = breezeRDD.map(p => p(variable).reduceResolution(5)).collect

    // we need to do this because the sciTensor might have many variables
    val underlying = collected(0).variables(variable).data.toList
    val breezeData = breezeCollect(0).variables(variable).data.toList
    //    println(underlying)
    //    println(breezeData)
    var i = 0
    var numInaccurate = 0
    while (i < underlying.length) {
      if ((Math.abs(underlying(i) - breezeData(i)) / underlying(i)) > 0.000001) {
        println(i + ": Underlying " + underlying(i) + " Breeze " + breezeData(i))
        numInaccurate += 1
      }
      i += 1
    }
    assert(numInaccurate == 0)
  }

  test("reduceResolutionTest") {
    val dense = new DenseMatrix[Double](180, 360, 1d to 64800d by 1d toArray, 0, 360, true)
    val nd = Nd4j.create(1d to 64800d by 1d toArray, Array(180, 360))
    val breeze = new BreezeTensor(dense)
    val nd4j = new Nd4jTensor(nd)
    //    println(breeze)

    //    println("rows" + dense(0, ::))
    //    println("rows" + nd.getRow(0))

    val b = breeze.reduceResolution(90)
    val n = nd4j.reduceResolution(90)

    println(b)
    println(n)

    println(b.data.toList)
    println(n.data.toList)

    //    println(breeze.data.toList)
    //    println(nd4j.data.toList)
  }
}
