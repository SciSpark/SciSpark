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

import java.util

import breeze.linalg.DenseMatrix
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Partition, TaskContext, Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.dia.NetCDFUtils
import org.dia.b.BreezeFuncs
import org.dia.n.Nd4jFuncs
import org.joda.time.DateTime

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * Created by marroquin on 7/13/15.
 */
sealed trait ArrayLib {
  def name: String
}

case object BREEZE extends ArrayLib {
  val name = "breeze"
}

case object ND4J extends ArrayLib {
  val name = "nd4j"
}

// TODO review usage of HashMap, it might be overcomplicating things
class sRDD[T: ClassTag](sc: SparkContext,
                        datasets: List[DataObject],
                        partitioner: (Any) => List[List[T]],
                        varName: String,
                        arrayLib: ArrayLib)
  extends RDD[T](sc, Nil) with Logging {

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   * Computes the iterator needed according to the array lib needed.
   */
  def compute(split: Partition, context: TaskContext): Iterator[T] = {
    getIterator(split.asInstanceOf[sRDDPartition])
  }

  def getIterator(theSplit: sRDDPartition): Iterator[T] = {
    val iterator = new Iterator[T] {
      var counter = 0

      override def hasNext: Boolean = {
        counter < theSplit.dataset.length
      }

      override def next(): T = {
        val tensor = theSplit.dataset(counter).asInstanceOf[DataObject].load(varName)
        counter += 1
        new DataObject(null, tensor).asInstanceOf[T]
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
    val listOfLists = partitioner(datasets)
    for (list <- listOfLists) {
      array(pos) = new sRDDPartition(pos, list)
      pos += 1
    }
    array
  }
}