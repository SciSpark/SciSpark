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
package org.dia.b

import breeze.linalg.DenseMatrix
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.dia.{NetCDFUtils, Constants}
import org.joda.time.{DateTime, Days}

import scala.StringBuilder
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * TrmmHourly partition
 */
class TrmmHourlyPartition(idx: Int, val date: DateTime, val readings: ListBuffer[String]) extends Partition {
  override def index: Int = idx
  override def toString() = {
    var sb = new StringBuilder()
    sb.append("{idx:").append(idx).append(", ");
    sb.append("date:").append(date).append(", ");
    sb.append("readings:").append(date).append("}");
    sb.toString()
  }
}

/**
 * TrmmHouly RDD abstraction
 * @param sc
 * @param datasetUrl
 * @param iniYear
 * @param finalYear
 * @param ev1
 * @tparam T
 */
class TrmmHourlyRDD[T: ClassTag](
                                  sc: SparkContext,
                                  datasetUrl: String,
                                  varName: String,
                                  iniYear: Int,
                                  finalYear: Int = 0)
  extends RDD[T](sc, Nil) with Logging {


  // partition by year-day.
  // Every day has around 96MB which is somewhat bigger than HDFS chunk
  override def getPartitions: Array[Partition] = {
    // get number of day's urls
    // 1. read from file and group readings by day
    val allReadings = HourlyTrmm.generateTrmmDaily(iniYear, finalYear)

    // 2. go to the web and get the results from there
    // TODO
    val result = new Array[Partition](allReadings.keySet.size)
    var cnt = 0
    allReadings.foreach(keyval =>
      result(cnt) = new TrmmHourlyPartition(cnt, keyval._1, keyval._2)
      {cnt+=1; cnt}
    )
    result
  }

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
      var splitt = split.asInstanceOf[TrmmHourlyPartition]
      println("============================")
      println("============================")
//      println(splitt.readings)
      println("============================")
      println("============================")
      val iter = new Iterator[T] {

        var hNext = true
        var counter = 0
        override def hasNext: Boolean = hNext

//        override def next(): (String, mutable.HashMap[String, DenseMatrix[Double]]) = null
        override def next(): T = {
          // for every reading fetch array
          if (counter >= splitt.readings.length)
            hNext = false
          else {
            println(splitt.readings(counter))
            counter +=1
          }
          val n = ""
          n.asInstanceOf[T]
        }
      }
      iter
  }
}
