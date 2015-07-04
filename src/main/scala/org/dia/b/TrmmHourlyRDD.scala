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

import org.apache.spark.rdd.RDD
import org.apache.spark.{TaskContext, Partition, SparkConf, SparkContext}
import org.dia.Constants
import org.joda.time.{DateTime, Days}

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * TrmmHourly partition
 */
class TrmmHourlyPartition(idx: Int, val date: String) extends Partition {
  override def index: Int = idx
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
class TrmmHourlyRDD[T: ClassTag](sc: SparkContext, datasetUrl: String, iniYear: Int, finalYear: Int) extends RDD[T](sc, Nil) {


  // partition by year-day.
  // Every day has around 96MB which is somewhat bigger than HDFS chunk
  override def getPartitions: Array[Partition] = {
    // get number of day's urls
    // 1. read from file
//    val numPartitions = HourlyTrmm.loadTrmmDaily(datasetUrl, iniYear, finalYear)
    val numPartitions = HourlyTrmm.loadTrmmDaily(Constants.TRMM_HOURLY_URL, 1997, 1997)
    // 2. go to the web and get the results from there
    // TODO
    val result = new Array[Partition](numPartitions.length)
    for (i <- 0 until numPartitions.length) {
      result(i) = new TrmmHourlyPartition(i, numPartitions(i))
    }
    result
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    throw new UnsupportedOperationException("empty RDD")
  }
}
