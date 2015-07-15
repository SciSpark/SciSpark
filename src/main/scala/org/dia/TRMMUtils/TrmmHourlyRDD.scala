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
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.dia.NetCDFUtils
import org.dia.TRMMUtils._
import org.joda.time.DateTime

import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 * TrmmHourly partition
 */
class TrmmHourlyPartition(
                           idx: Int,
                           val date: DateTime,
                           val readings: ListBuffer[String])
  extends Partition {

  /**
   * Partition index
   */
  override def index: Int = idx

  /**
   * To string method
   * @return String
   */
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
class TrmmHourlyRDD[T: ClassTag](sc: SparkContext,
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
    // TODO check if url exist to not to create an empty partition
    val result = new Array[Partition](allReadings.keySet.size)
    logDebug("Found %d days/partitions.".format(allReadings.keySet.size))
    var cnt = 0
    allReadings.foreach(keyval =>
      result(cnt) = new TrmmHourlyPartition(cnt, keyval._1, keyval._2) {
        logDebug("Partition created %s".format(result(cnt)))
        cnt += 1;
        cnt
      }
    )
    result
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    // turning split into a split we understand
    var splitt = split.asInstanceOf[TrmmHourlyPartition]
    // creating our own iterator
    val iter = new Iterator[T] {

      var counter = 0
      var hNext = true

      override def hasNext: Boolean = {
        logDebug("Iterating through element %d out of %d".format(counter, splitt.readings.length))
        counter < splitt.readings.length
      }

      // TODO fix the class type, we know what it'll be
      override def next(): T = {
        // for every reading fetch array
        var n = datasetUrl + "/" + splitt.date.getYear + "/" + "%03d".format(splitt.date.getDayOfYear) + "/" + splitt.readings(counter)
        var netCdfFile = NetCDFUtils.loadNetCDFDataSet(n)
        var twoDarray: DenseMatrix[Double] = null
        //        var twoDarray = DenseMatrix.zeros[Double](300, 300)
        if (netCdfFile != null) {
          logInfo("Reading from %s".format(n))
          try {
            var dimensionSizes = NetCDFUtils.getDimensionSizes(netCdfFile, varName)
            twoDarray = BreezeFuncs.create2dArray(dimensionSizes, netCdfFile, varName)
          } catch {
            case e: Exception => logError("ERROR reading variable %s from %s".format(varName, n))
          }
        }
        counter += 1

        if (counter >= splitt.readings.length)
          hNext = false
        (n, twoDarray).asInstanceOf[T]
      }
    }
    // returning our iterator
    iter
  }
}
