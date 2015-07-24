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

import breeze.linalg.DenseMatrix
import org.dia.tensors.BreezeTensor
import org.dia.core.{sciTensor, SciSparkContext}

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
    //val cores = Runtime.getRuntime().availableProcessors() - 1;
    //TODO the number of threads should be configured at cluster level
    val scisparkContext = new SciSparkContext("local[4]", "test")
    val ArrayList = new BreezeTensor(DenseMatrix.zeros[Double](289, 555))
    val s = new BreezeTensor(DenseMatrix.zeros[Double](289, 555))
    val x = s + s
    println(x)
    //val HighResolutionArray = scisparkContext.OpenDapURLFile("TestLinks", "TotCldLiqH2O_A")

    /**
     * Uncomment this line in order to test on a normal scala array
     * val urlRDD = Source.fromFile("TestLinks").mkString.split("\n")
     */

//    val LowResolutionArray = HighResolutionArray.map(largeArray => Nd4jFuncs.reduceResolution(largeArray, 5)).collect
//    val MaskedArray = LowResolutionArray.map(array => BooleanIndexing.applyWhere(array, Conditions.lessThanOrEqual(241.toDouble), Identity))
    //val collected = HighResolutionArray.collect
//    MaskedArray.map(p => println(p))
  }
}


