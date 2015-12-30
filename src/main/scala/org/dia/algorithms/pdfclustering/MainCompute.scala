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

    //    val netcdfFile = NetCDFUtils.loadNetCDFDataSet(uri)
    //    val variables = netcdfFile.getVariables
    //    val numVars = variables.size()
    //    var idx = 0
    //    while (idx < numVars) {
    //      val variable = variables.get(idx)
    //      println(variable)
    //      idx += 1
    //    }

    //    val surfTemp = NetCDFReader.loadNetCDFNDVar(uri, variable)
    //    println("surfTemp: " + surfTemp._2.deep.mkString(" "))

    val masterURL = "local[2]"

    val sc = new SciSparkContext(masterURL, "PDF clustering")

    val rdd = sc.NetcdfDFSFile(netcdfDir, variables, partCount)
    val fst = rdd.collect()(0)
    
    
  }

}