/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dia.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.dia.tensors.{Nd4jTensor, BreezeTensor}
import org.nd4j.linalg.factory.Nd4j

import scala.reflect.ClassTag

class sRDD[T: ClassTag](sc: SparkContext,
                        datasets: List[String],
                        varName: String,
                        loadFunc: (String, String) => (Array[Double], Array[Int]),
                        partitionFunc: List[String] => (List[List[String]])
                         )

  extends RDD[T](sc, Nil) with Logging {

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   * Computes the iterator needed according to the array lib needed.
   */
  def compute(split: Partition, context: TaskContext): Iterator[T] = {
    // call the loader/constructor
    getIterator(split.asInstanceOf[sRDDPartition[T]])
  }

  def getIterator(theSplit: sRDDPartition[T]): Iterator[T] = {

    val iterator = new Iterator[T] {
      var counter = 0

      //
      override def hasNext: Boolean = {
        counter < theSplit.tensors.length
      }

      //
      override def next(): T = {
        val urlValue = theSplit.tensors(counter)
        counter += 1
        val loader = () => {loadFunc(urlValue, varName)}
        val abstracttensor = new sTensor(new Nd4jTensor(loader))
        abstracttensor.asInstanceOf[T]
      }
    }
    iterator
  }

  /**
   *
   * Returns the set of partitions in this RDD. Each partition represents a single URLs.
   * The default setting is a grouping of 1 url.
   *
   * @return
   */
  override def getPartitions: Array[Partition] = {
    var pos = 0
    val array = new Array[Partition](datasets.length)
    // will create a list of lists of empty sTensors
    val listOfLists = partitionFunc(datasets)
    for (list <- listOfLists) {
      array(pos) = new sRDDPartition(pos, list)
      pos += 1
    }
    array
  }

}