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

import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.dia.core.{ND4J, sRDD}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * Created by rahulsp on 7/8/15.
 */
class sciNd4jRDD[T: ClassTag](sc: SparkContext,
                              datasets: mutable.HashMap[_,_],
                              callback: (Any, Any) => ListBuffer[String],
                              varName: String)
  extends sRDD[T](sc, datasets, callback, varName, ND4J) with Logging {

  /**
   * Computes the sRDD elements by pulling them from the OpenDap URLs
   * TODO :: Decouple sRDD compute from the source url - instead use NetCDFDataset
   * @param theSplit
   * @param context
   * @return
   */
  override def compute(theSplit: Partition, context: TaskContext): Iterator[T] = {
    val split = theSplit.asInstanceOf[sRDDPartition]
    val iterator = new Iterator[T] {
      var counter = 0

      override def hasNext: Boolean = {
        counter < split.dataset.length
      }

      override def next: T = {
//        val tensor = Nd4jFuncs.getNetCDFNDVars(split.datasets(counter), varName)
//        counter += 1
//        tensor.asInstanceOf[T]
        null.asInstanceOf[T]
      }
    }

    iterator
  }
}
