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
 */
object RandomMatrixLoader {

    def loadRandomArray(url : String, varname : String) : (Array[Double], Array[Int]) = {
      val generator = new Random()
      generator.setSeed(url.hashCode)
      val randomCenter = generator.nextDouble * 20.0
      val randomCenterOther = generator.nextDouble * 20
      val otroRandomCenter = generator.nextDouble * 20
      val ndArray = Nd4j.zeros(20, 20)
      for(row <- 0 to ndArray.rows - 1){
        for(col <- 0 to ndArray.columns - 1){
          if (Math.pow(row - randomCenter, 2) + Math.pow(col - randomCenter, 2) <= 9) ndArray.put(row, col, generator.nextDouble * 340)
          if (Math.pow(row - randomCenterOther, 2) + Math.pow(col - randomCenterOther, 2) <= 9) ndArray.put(row, col, generator.nextDouble * 7000)
          if (Math.pow(row - otroRandomCenter, 2) + Math.pow(col - otroRandomCenter, 2) <= 9) ndArray.put(row, col, generator.nextDouble * 24000)
        }
      }
      (ndArray.data.asDouble, ndArray.shape)
    }

  def loadRandomArray(sizeTuple: (Int, Int))(url: String, varname: String): (Array[Double], Array[Int]) = {
    val generator = new Random()
    generator.setSeed(url.hashCode)
    val randomCenter = generator.nextDouble * sizeTuple._1
    val randomCenterOther = generator.nextDouble * sizeTuple._1
    val otroRandomCenter = generator.nextDouble * sizeTuple._1
    val ndArray = Nd4j.zeros(sizeTuple._1, sizeTuple._1)
    for (row <- 0 to ndArray.rows - 1) {
      for (col <- 0 to ndArray.columns - 1) {
        if (Math.pow(row - randomCenter, 2) + Math.pow(col - randomCenter, 2) <= 9) ndArray.put(row, col, generator.nextDouble * 340)
        if (Math.pow(row - randomCenterOther, 2) + Math.pow(col - randomCenterOther, 2) <= 9) ndArray.put(row, col, generator.nextDouble * 7000)
        if (Math.pow(row - otroRandomCenter, 2) + Math.pow(col - otroRandomCenter, 2) <= 9) ndArray.put(row, col, generator.nextDouble * 24000)
      }
    }
    (ndArray.data.asDouble, ndArray.shape)
  }
}
