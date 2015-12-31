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
package org.dia.algorithms.pdfclustering

import org.dia.loaders.NetCDFReader
import org.dia.utils.NetCDFUtils
import ucar.nc2.dataset.NetcdfDataset
import org.dia.core.SciSparkContext
import java.text.SimpleDateFormat
import org.dia.Parsers
import org.dia.core.SciTensor
import org.dia.tensors.{ Nd4jTensor, AbstractTensor }
import org.nd4j.linalg.factory.Nd4j
import scala.collection.mutable.Set
import scala.math

object MainCompute {

  def main(args: Array[String]): Unit = {

    val partCount = 2

    val netcdfDir = "resources/multisen"
    val numYears = 3
    val numJanuaryDays = 2
    val totNumDays = numYears * numJanuaryDays
    val numLats = 720
    val numLongs = 1440
    val numHourly = 8

    /**
     *  prec(time,lat,lon)
     */
    val variables = List("lat", "lon", "prec")

    val masterURL = "local[2]"

    val sc = new SciSparkContext(masterURL, "PDF clustering")

    /**
     * Each SciTensor is
     * prec -> prec(time=8,lat=720,lon=1440)
     * lat -> lat(lat=720)
     * lon -> lon(lon=1440)
     * with meta data
     * SOURCE -> file:[absolute local FS path of NetCDF]
     */
    val rddStart = sc.NetcdfDFSFile(netcdfDir, variables, partCount)

    /**
     * Equip SciTensors with additional meta data
     * DAYOFJANUARY -> Day of January
     */
    val rddStartWithDate = rddStart.map(t => {
      val fileName = t.metaData("SOURCE").split("/").last
      val lastPart = fileName.split("_").last
      val dayOfJan = lastPart.substring(6, 8)
      t.insertDictionary(("DAYOFJANUARY", dayOfJan))
      t
    })

    /**
     * Average the hourly precipitation amounts to daily amounts.
     * Each SciTensor now is
     * prec -> prec(lat=720,lon=1440)
     * lat -> lat(lat=720)
     * lon -> lon(lon=1440)
     * with meta data
     * SOURCE -> file:[absolute local FS path of NetCDF]
     * DAYOFJANUARY -> Day of January (i.e. "01" - "31")
     */
    val dayAvgs = rddStartWithDate.map({ t =>
      /**
       * Averages the SciTensor along a dimension.
       *
       * @todo abstract this out into AbstractTensor
       * @todo generalize "prec" field and dims (720,1440)
       * @todo can we get rid of explicit 'AbstractTensor's here?
       * also need to maybe slightly modify AbstractTensor interface
       * to achieve that.
       */
      def avgAlongDim(st: SciTensor): AbstractTensor = {
        val prec = st.variables("prec")
        val dummy: AbstractTensor = prec((0, 0))
        println("Attention dummy: " + dummy.shape.deep.mkString(" "))
        val avg: AbstractTensor = dummy.zeros(Seq(numLats, numLongs): _*)
        println("Attention avg: " + avg.shape.deep.mkString(" "))
        var idx = 0
        var lats = 0
        val longs = 0
        while (idx < numHourly) {
          while (lats < numLats) {
            while (longs < numLongs) {
              val curSum = avg(lats,longs)
              val newSum = curSum + prec(idx,lats,longs)
              avg.put(newSum, Seq(lats, longs): _*)   
            }
          }
          println("Attention slice: " + prec((idx, idx)).shape.deep.mkString(" "))
          idx += 1
        }
        avg.div(8)
      }
      val precAvg = avgAlongDim(t)
      /** overwrites prec with precAvg but keeps lat,lon tensors */
      t.insertVar("prec", precAvg)
      t
    })

    val groupedByJanDay = dayAvgs.groupBy({ t =>
      t.metaData("DAYOFJANUARY")
    })

    val subAvgByDay = groupedByJanDay.flatMap({
      case (day, ts) =>
        def compAvg(tensors: Iterable[SciTensor]): AbstractTensor = {
          val precs = tensors.map(_.variables("prec"))
          val sumPrecs = precs.reduce(_ + _)
          sumPrecs.div(numYears)
        }
        val avgPrec = compAvg(ts)
        ts.map({ t =>
          t.insertVar("prec", t.variables("prec") - avgPrec)
          t
        })
    })

    val totalAvg = subAvgByDay.map(_.variables("prec")).reduce(_ + _).div(totNumDays)

    val subTotalAvg = subAvgByDay.map({ t =>
      t.insertVar("prec", t.variables("prec") - totalAvg)
      t
    })

    val timeSeriesPerLatLon = subTotalAvg.flatMap({ t =>
      val vars = t.variables
      val lats = vars("lat")
      val longs = vars("lon")
      val prec = vars("prec")
      /** ( (lat,lon), time series ) */
      var idx = 0
      var jdx = 0
      var precPerLatLon: Set[((Double, Double), Double)] = Set()
      while (idx < numLats) {
        while (jdx < numLongs) {
          val elem = ((lats(idx), longs(jdx)), prec(idx, jdx))
          precPerLatLon += elem
          jdx += 1
        }
        idx += 1
      }
      precPerLatLon
    })

    var accumMin = sc.sparkContext.accumulator(Double.MaxValue, "Minimum precipitation")
    var accumMax = sc.sparkContext.accumulator(Double.MinValue, "Maximum precipitation")
    timeSeriesPerLatLon.foreach({
      case (_, prec) =>
        if (prec < accumMin.value)
          accumMin.setValue(prec)
        if (prec > accumMax.value)
          accumMax.setValue(prec)
    })

    val range = accumMax.value - accumMin.value
    val binSize = 0.1
    val numBins = range / binSize
    println(numBins)

  }

}