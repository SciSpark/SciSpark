package org.dia.core

import org.apache.spark.storage.StorageLevel
import org.dia.Constants
import Constants._
import org.dia.TRMMUtils.HourlyTrmmUrlGenerator
import org.scalatest.FunSuite

import scala.collection.mutable.{HashMap, ListBuffer}
import org.dia.loaders.NetCDFLoader._
import org.dia.core.sPartitioner._
import scala.io.Source

/**
 * Tests for creating different Rdd types.
 * Created by marroquin on 7/14/15.
 */
class sRDDTest extends FunSuite  {
  test("SimplePartitionScheme") {
    val dataUrls = Source.fromFile("TestLinks").mkString.split("\n").toList
    val sc = new SciSparkContext("local[4]", "test")

    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)
    val sBreezeRdd = new sRDD[sciTensor] (sc, dataUrls, "TotCldLiqH2O_A", loadNetCDFNDVars, mapOneUrlToOneTensor)
    var start = System.nanoTime()
    sBreezeRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    sBreezeRdd.collect()
    var end = System.nanoTime()
    println("====")
    println((end-start)/1000000000.0)
    println("====")

    sc.setLocalProperty(ARRAY_LIB, ND4J_LIB)
    val sNd4jRdd = new sRDD[sciTensor] (sc, dataUrls, "TotCldLiqH2O_A", loadNetCDFNDVars, mapOneUrlToOneTensor)
    start = System.nanoTime()
    sNd4jRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    sNd4jRdd.collect()
    end = System.nanoTime()
    println("====")
    println((end-start)/1000000000.0)
    println("====")
//
//    sRdd.filter().map(element => ND4J.re...)
//    sRdd.filter().map(element => element.ndarray)
//
//    println()
//    println(sRdd.collect().length)
//    println()
//    sc.stop
  }

  test("GroupingByDayPartitioning") {
    val dataMapping = HourlyTrmmUrlGenerator.generateTrmmDaily(1999)
    val sc = SparkTestConstants.sc
//    val sRdd = new sRDD[HashMap[String, DenseMatrix[Double]]] (sc, dataMapping, sPartitioner.mapOneUrlToOneTensor, "precipitation", BREEZE)
//    val sRdd = new sRDD[HashMap[String, INDArray]] (sc, dataMapping, sPartitioner.mapOneUrlToOneTensor, "precipitation", ND4J)
//    println()
//    println(sRdd.collect().length)
//    println()
    sc.stop
  }

  test("GroupingByMonthPartitioning") {
    var dataUrls = HourlyTrmmUrlGenerator.generateTrmmDaily(1999)
    val dataMapping = new HashMap[String, HashMap[String, ListBuffer[String]]]()
    for ((key, value) <- dataUrls) {
      val newKey = key.toString("yyyy-MM")
      var keyDay = new HashMap[String, ListBuffer[String]]
      if (dataMapping.get(newKey) != None) {
        keyDay = dataMapping.get(newKey).get
      }
      keyDay.put(key.getDayOfYear.toString, value)
      dataMapping.put(newKey, keyDay)
    }
    for ((k,v) <- dataMapping) {
      println(v.keySet)
    }
    val sc = SparkTestConstants.sc
//    val srdd = new sciBreezeRDD[DenseMatrix[Double]] (sc, dataMapping, "TotCldLiqH2O_A")
  }

  test("GroupingByYearPartitioning") {
    var dataUrls = HourlyTrmmUrlGenerator.generateTrmmDaily(1999,2000)
    val dataMapping = new HashMap[String, HashMap[String, ListBuffer[String]]]()
    for ((key, value) <- dataUrls) {
      val newKey = key.toString("yyyy")
      var keyYear= new HashMap[String, ListBuffer[String]]
      if (dataMapping.get(newKey) != None)
        keyYear = dataMapping.get(newKey).get
      dataMapping.put(newKey, keyYear)
    }
    //    val data = dataUrls.keySet.foreach(elem => println(elem.getYear))
    println(dataMapping)
  }

  test("BreezeRdd.basic") {
//    val sc = SparkTestConstants.sc
//    val datasetUrls = Source.fromFile("TestLinks").mkString.split("\n").toList
//    val datasetMapping = datasetUrls.foreach(element => (0, element)).asInstanceOf[Map[AnyVal, Any]]
//    val srdd = new sciBreezeRDD[DenseMatrix[Double]] (sc, datasetMapping, "TotCldLiqH2O_A")

//    val collected = srdd.collect
//    collected.map(p => println(p))
//    sc.stop()
    assert (true)
  }

  test("Nd4jRdd.basic") {
//    val sc = SparkTestConstants.sc
//    val datasetUrls = Source.fromFile("TestLinks").mkString.split("\n").toList
//    val datasetMapping = datasetUrls.foreach(element => (0, element)).asInstanceOf[Map[AnyVal, Any]]
//    val srdd = new sciNd4jRDD[INDArray](sc, datasetMapping, "TotCldLiqH2O_A")

//    val collected = srdd.collect
//    collected.map(p => println(p))
//    sc.stop()
    assert(true)
  }

  test("testOpenDapURLFile") {
//    val sciContext = SparkTestConstants.sc
//    val srdd = sciContext.OpenDapURLFile("TestLinks", "TotCldLiqH2O_A")
//
//    val collected = srdd.collect
//    collected.map(p => println(p))
    assert(true)
  }
}