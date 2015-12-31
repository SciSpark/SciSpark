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

object MainCompute {

  def main(args: Array[String]): Unit = {

    val partCount = 2

    val netcdfDir = "resources/multisen"

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
     * Build RDD[SciTensor] where SciTensor = T(time,lat,lon) with
     * time = only hours over one day + meta = year:dayOfJanuary
     */
    //    val rddByDay = rddStart.flatMap(netCDF => {
    //      /** slice out hours for first day */
    //      val hrsDay1 = netCDF((0, 11))
    //      /** slice out hours for second day */
    //      val hrsDay2 = netCDF((11, 23))
    //      List(hrsDay1, hrsDay2)
    //    })

    val collected = rddStartWithDate.collect()(0)
    println(collected.metaData)

  }

}