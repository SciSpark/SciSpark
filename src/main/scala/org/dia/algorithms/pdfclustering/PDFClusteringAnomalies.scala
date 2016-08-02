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

import java.util

import org.dia.core.SciSparkContext
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import ucar.ma2.{ArrayInt, ArrayDouble, DataType}
import ucar.nc2.{Dimension, NetcdfFileWriter}
object PDFClusteringAnomalies {

  def whiten(input: Array[Double]): Array[Double] = {
      val mean = input.reduce((a, b) => a + b) / input.length
      val summation = input.map(p => Math.pow(p - mean, 2.0)).reduce((a, b) => a + b)
      val std = Math.sqrt(summation / input.length)
      input.map(p => p / std)
  }

  def main(args: Array[String]): Unit = {
    val nyears = 33
    val mdays = 31
    val lat = 181
    val lon = 286
    val masterURL = "local[2]"
    val ncfile = "resources/PDFClustering/tas_MERRA_NA_daily_January_1979-2011.nc"
    val ncvariables = List("tasjan", "lat", "lon")
    val sc = new SciSparkContext(masterURL, "PDF clustering")
    val tasjan = sc.NetcdfDFSFile(ncfile, ncvariables)

    /**
     * Detrend daily data
     */
    val detrendedData = tasjan.map(p => {
      var data = p("tasjan").copy
      data = data.reshape(Array(nyears, mdays, lat, lon))
      val clim = data.mean(0).reshape(Array(1, mdays, lat, lon))

      // broadcast subtract
      // Since subtraction is now done in place by default
      // The results are in the 'data' array
      for(i <- 0 until nyears) {
        data(i -> (i + 1)) - clim
      }
      data = data.reshape(Array(nyears*mdays, lat, lon))
      val detrended = data.detrend(Array(0))
      detrended.insertVar("lat", p("lat").tensor)
      detrended.insertVar("lon", p("lon").tensor)
      detrended
    })

    val calc_moments = detrendedData.map(p => {
      print(p)
      val std = p.std(Array(0)).tensor
      val skw = p.skew(Array(0)).tensor
      p.insertVar("std", std)
      p.insertVar("skw", skw)
      p
    }).collect

    val std = calc_moments(0)("std").tensor.data
    val skw = calc_moments(0)("skw").tensor.data
    val latarr = calc_moments(0)("lat").tensor.data
    val lonarr = calc_moments(0)("lon").tensor.data
    val zipped = std.zip(skw)

    val numClusters = 5
    val iter = 300

    val vectors = sc.sparkContext.parallelize(zipped).map{ case (std_i, sk_i) => Vectors.dense(std_i, sk_i)}

    val clusters = new KMeans().setSeed(0).setMaxIterations(iter).setK(numClusters).run(vectors)
    val centers = clusters.clusterCenters.toList
    val prd = clusters.predict(vectors).collect

    val writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, "results", null)
    val xDim = writer.addDimension(null, "len", std.length)
    val centerDim = writer.addDimension(null, "len_centers", centers.length)
    val latDim = writer.addDimension(null, "latDim", latarr.length)
    val lonDim = writer.addDimension(null, "lonDim", lonarr.length)
    val dims = new util.ArrayList[Dimension]()
    val cDim = new util.ArrayList[Dimension]()
    val latDims = new util.ArrayList[Dimension]()
    val lonDims = new util.ArrayList[Dimension]()
    dims.add(xDim)
    cDim.add(centerDim)
    latDims.add(latDim)
    lonDims.add(lonDim)
    val std_var = writer.addVariable(null, "std", DataType.DOUBLE, dims)
    val skw_var = writer.addVariable(null, "skw", DataType.DOUBLE, dims)
    val prd_var = writer.addVariable(null, "prediction", DataType.INT, dims)
    val cnt_std_var = writer.addVariable(null, "centers_std", DataType.DOUBLE, cDim)
    val cnt_skw_var = writer.addVariable(null, "centers_skw", DataType.DOUBLE, cDim)
    val lat_dim_var = writer.addVariable(null, "lat", DataType.DOUBLE, latDims)
    val lon_dim_var = writer.addVariable(null, "lon", DataType.DOUBLE, lonDims)
    writer.create()

    val stddataOut = new ArrayDouble.D1(std.length)
    val skwdataOut = new ArrayDouble.D1(skw.length)
    val prdDataOut = new ArrayInt.D1(prd.length)
    val cnt_stdDataOut = new ArrayDouble.D1(centers.length)
    val cnt_skwDataOut = new ArrayDouble.D1(centers.length)
    val latdataOut = new ArrayDouble.D1(latarr.length)
    val londataOut = new ArrayDouble.D1(lonarr.length)
    assert(std.length == skw.length && skw.length == prd.length)
    for(i <- 0 until std.length) {
      stddataOut.set(i, std(i))
      skwdataOut.set(i, skw(i))
      prdDataOut.set(i, prd(i))
    }

    for(i <- 0 until centers.length) {
      cnt_stdDataOut.set(i, centers(i)(0))
      cnt_skwDataOut.set(i, centers(i)(1))
    }

    for(i <- 0 until latarr.length) {
      latdataOut.set(i, latarr(i))
    }

    for(i <- 0 until lonarr.length) {
      londataOut.set(i, lonarr(i))
    }
    writer.write(std_var, stddataOut)
    writer.write(skw_var, skwdataOut)
    writer.write(prd_var, prdDataOut)
    writer.write(lat_dim_var, latdataOut)
    writer.write(lon_dim_var, londataOut)
    writer.write(cnt_std_var, cnt_stdDataOut)
    writer.write(cnt_skw_var, cnt_skwDataOut)

    writer.close()
  }


  /**
    * (1 to 1000d).toArray.reduce((A, B) => (
    *
    *
    *
    */
}
