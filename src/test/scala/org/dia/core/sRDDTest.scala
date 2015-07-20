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
  test("ArrayLibsSanityTest") {
    val dataUrls = Source.fromFile("TestLinks").mkString.split("\n").toList
    val sc = SparkTestConstants.sc

    // Breeze library
    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)
    val sBreezeRdd = new sRDD[sciTensor] (sc, dataUrls, "TotCldLiqH2O_A", loadNetCDFNDVars, mapOneUrlToOneTensor)
    sBreezeRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    var start = System.nanoTime()
    val breezeTensors = sBreezeRdd.collect()
    var end = System.nanoTime()
    var breezeTime = (end - start)/1000000000.0

    // Nd4j library
    sc.setLocalProperty(ARRAY_LIB, ND4J_LIB)
    val sNd4jRdd = new sRDD[sciTensor] (sc, dataUrls, "TotCldLiqH2O_A", loadNetCDFNDVars, mapOneUrlToOneTensor)
    sNd4jRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    start = System.nanoTime()
    val nd4jTensors = sNd4jRdd.collect()
    end = System.nanoTime()
    var nd4jTime = (end - start)/1000000000.0

    // element comparison
    var flg = true
    var cnt = 0
    nd4jTensors(0).tensor.data.map(e => {
      if(e != breezeTensors(0).tensor.data(cnt))
        flg = false
      cnt+=1
    })

    // printing out messages
    println("BREEZE : %.6f".format(breezeTime))
    println("ND4J : %.6f".format(nd4jTime))
    println("EQUAL ELEMENTS? %b".format(flg))
  }

  test("GroupingByDayPartitioning") {
    val dataMapping = HourlyTrmmUrlGenerator.generateTrmmDaily(1999)
    val sc = SparkTestConstants.sc
//    val sNd4jRdd = new sRDD[sciTensor] (sc, dataUrls, "TotCldLiqH2O_A", loadNetCDFNDVars, mapOneUrlToOneTensor)

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