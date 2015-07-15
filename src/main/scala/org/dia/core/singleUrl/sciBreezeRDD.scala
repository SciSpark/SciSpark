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
package org.dia.core.singleUrl

import breeze.linalg.DenseMatrix
import org.apache.spark.{TaskContext, Partition, Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.dia.NetCDFUtils
import org.dia.b.BreezeFuncs
import org.dia.core.{BREEZE, sRDD}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * Created by marroquin on 7/13/15.
 */
class sciBreezeRDD [T: ClassTag](sc: SparkContext,
                                 datasets: mutable.HashMap[_,_],
                                 callback: (Any, Any) => ListBuffer[String],
                                 varName: String)
  extends sRDD[T](sc, datasets, callback, varName, BREEZE) with Logging {

  /**
   * Computes the sRDD elements by pulling them from the OpenDap URLs
   * @param split
   * @param context
   * @return
   */
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val theSplit = split.asInstanceOf[sRDDPartition]
    val iterator = new Iterator[T] {
      var counter = 0

      override def hasNext: Boolean = {
        counter < theSplit.dataset.length
      }

      override def next: T = {
        var resultHashMap = new mutable.HashMap[String,ListBuffer[DenseMatrix[Double]]]()
        var readings = new ListBuffer[DenseMatrix[Double]]
        //TODO better error handling
        for(elem :String <- theSplit.dataset) {
          val netcdfFile = NetCDFUtils.loadNetCDFDataSet(elem)
          val dimensions = NetCDFUtils.getDimensionSizes(netcdfFile, varName)
          val two2dArray = BreezeFuncs.create2dArray(dimensions, netcdfFile, varName)
          readings+=two2dArray
        }
        resultHashMap.put(theSplit.partId, readings)
        counter += 1
        (resultHashMap).asInstanceOf[T]
      }
    }
    iterator
  }

}
