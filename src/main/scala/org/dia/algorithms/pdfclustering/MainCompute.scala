package org.dia.algorithms.pdfclustering

import org.dia.loaders.NetCDFReader
import org.dia.utils.NetCDFUtils
import ucar.nc2.dataset.NetcdfDataset
import org.dia.core.SciSparkContext

object MainCompute {

  def main(args: Array[String]): Unit = {

    val partCount = 2

    val netcdfDir = "resources/merra"
    /** TSURF(time, lat, lon) */
    val variables = List("TSURF")

    //    val vars = NetCDFReader.loadNetCDFVar(uri)
    //    val lon = NetCDFReader.loadNetCDFNDVar(uri, "lon")
    //    val lat = NetCDFReader.loadNetCDFNDVar(uri, "lat")
    //    val lev = NetCDFReader.loadNetCDFNDVar(uri, "lev")
    //    val time = NetCDFReader.loadNetCDFNDVar(uri, "time")
    //    val surfTemp = NetCDFReader.loadNetCDFNDVar(uri, "T")

    //    val netCDF = NetCDFUtils.loadNetCDFDataSet(url)
    //    val netCDF = NetcdfDataset.openDataset(url)
    //    println(vars)
    //    println("long: " + lon._2.deep.mkString(" "))
    //    println("lat: " + lat._2.deep.mkString(" "))
    //    println("lev: " + lev._2.deep.mkString(" "))
    //    println("time: " + time._2.deep.mkString(" "))
    //    println("surfTemp: " + surfTemp._2.deep.mkString(" "))

    //    vars.foreach { v =>
    //      println(v + ": " + NetCDFReader.loadNetCDFNDVar(uri, v)._2.deep.mkString(" "))
    //    }

    //    /** This is just to check the variables' meta info */
    //    val uri = "resources/merra/MERRA2_400.tavgU_2d_lnd_Nx.201101.nc4"
    //    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(uri)
    //    val someVars = netcdfFile.getVariables
    //    val numVars = someVars.size()
    //    var idx = 0
    //    while (idx < numVars) {
    //      val variable = someVars.get(idx)
    //      println(variable)
    //      idx += 1
    //    }

    //    val surfTemp = NetCDFReader.loadNetCDFNDVar(uri, variable)
    //    println("surfTemp: " + surfTemp._2.deep.mkString(" "))

    val masterURL = "local[2]"

    val sc = new SciSparkContext(masterURL, "PDF clustering")

    /**
     *  Each SciTensor of this RDD here is of the form T(time,lat,lon)
     *  where T = surface temperature.
     */
    val rdd = sc.NetcdfDFSFile(netcdfDir, variables, partCount)

    //    /** some plausibility checks */
    //    val rddCollected = rdd.collect()
    //    println(rddCollected.length)
    //    val firstTensorShape = rddCollected(0).variables.get("TSURF").map(_.shape)
    //    val output = firstTensorShape match {
    //      case Some(shape) => shape.deep.mkString(" ")
    //      case _ => "NONE"
    //    }
    //    /** should print 24 361 576 (time lat lon) */
    //    println(output)

    /**
     * This will give us an RDD with SciTensor's of the form
     * metaInfo: (lat,lon) + data: TSURF -> T(time).
     * This is the complicated + time-consuming part (because
     * of network traffic) of the entire clustering algorithm.
     */
//    val reshapedRDD = rdd

  }

}