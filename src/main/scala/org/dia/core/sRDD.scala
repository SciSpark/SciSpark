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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.{Partition, TaskContext, Logging, SparkContext}
import org.apache.spark.rdd.RDD
import org.dia.core.singleUrl.{sRDDPartition, sciBreezeRDD, sciNd4jRDD}
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

abstract class sRDD[T: ClassTag](sc: SparkContext,
                                 datasets: mutable.HashMap[_, _],
                                 callback: (Any, Any) => ListBuffer[String],
                                 varName: String,
                                 arrayLib: ArrayLib)
  extends RDD[T](sc, Nil) with Logging {

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  @DeveloperApi
  def compute(split: Partition, context: TaskContext): Iterator[T]

  /**
   * Return a new RDD containing only the elements that satisfy a predicate.
   */
//  def mapOneUrlPartition(): RDD[T] = {
//    arrayLib match {
//            case ND4J => new sciNd4jRDD[T](sc, datasets, varName)
//      case BREEZE => new sciBreezeRDD[T](sc, datasets, callback, varName)
//      case _ => throw new IllegalArgumentException("Array library not supported.")
//    }
//
//  }

  /**
   *
   * Returns the set of partitions in this RDD. Each partition represents a single URLs.
   * The default setting is a grouping of 1 url.
   *
   * @return
   */
  override def getPartitions: Array[Partition] = {
    var pos = 0
    val array = new Array[Partition](datasets.keySet.size)
    for ((key, value) <- datasets) {
      array(pos) = new sRDDPartition(pos, key.toString, callback(key, value))
      pos += 1
    }
    array
  }
}