package org.dia.loaders

import java.io.File

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.dia.Constants._
import org.dia.TRMMUtils.HourlyTrmmUrlGenerator
import org.dia.core.{sciTensor, sRDD, SparkTestConstants}
import org.dia.loaders.NetCDFLoader._
import org.dia.loaders.PathUtils.mapSubFoldersToFolders
import org.dia.partitioners.sPartitioner._
import org.dia.partitioners.sTrmmPartitioner._

/**
 * Created by marroquin on 7/22/15.
 */
class LoadersTest extends org.scalatest.FunSuite {

  test("RecursiveFileListing") {
    val path ="src/main/scala/"
    val files = PathUtils.recursiveListFiles(new File(path))
    println("Found: %d sub-directories.".format(files.size))
    files.map(vals => {
      if (vals._2.size > 0) {
        vals._2.map(e => println(e)); println;
      } else {
        println("Empty")
      }
    })
    assert(true)
  }

  test("LoadPathGrouping") {
    val dataUrls = List("/Users/marroqui/Documents/projects/scipark/data/TRMM_3Hourly_3B42_1998/")
    val sc = SparkTestConstants.sc
    sc.getConf.set("log4j.configuration", "resources/log4j-defaults.properties")
    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)

    val sBreezeRdd = new sRDD[sciTensor] (sc, dataUrls, "precipitation", loadNetCDFNDVars, mapSubFoldersToFolders)
    sBreezeRdd.collect
    assert(true)
  }

  test("OpenLocalPath") {
    val sc = SparkTestConstants.sc
    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)
    var path = "/Users/marroqui/Documents/projects/scipark/data/TRMM_3Hourly_3B42_1998/"
    val pathRDD : sRDD[sciTensor] = sc.OpenPath(path, "precipitation")
    println(pathRDD.collect()(0).data.size)
    assert(true)
  }
}
