package org.dia.core

import org.dia.TRMMUtils.HourlyTrmm
import org.scalatest.FunSuite

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.io.Source

/**
 * Tests for creating different Rdd types.
 * Created by marroquin on 7/14/15.
 */
class sRDDTest extends FunSuite  {
  test("SimplePartitionScheme") {
    val sc = SparkTestConstants.sc
    val dataUrls = Source.fromFile("TestLinks").mkString.split("\n").toList
    val sRdd = new sRDD[DataObject] (sc, dataUrls, "TotCldLiqH2O_A", "Breeze")
    val sRdsd = new sRDD[DataObject] (sc, dataUrls, "TotCldLiqH2O_A", "Nd4j")

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
    val dataMapping = HourlyTrmm.generateTrmmDaily(1999)
    val sc = SparkTestConstants.sc
//    val sRdd = new sRDD[HashMap[String, DenseMatrix[Double]]] (sc, dataMapping, Groupers.mapDayUrls, "precipitation", BREEZE)
//    val sRdd = new sRDD[HashMap[String, INDArray]] (sc, dataMapping, Groupers.mapDayUrls, "precipitation", ND4J)
//    println()
//    println(sRdd.collect().length)
//    println()
    sc.stop
  }

  test("GroupingByMonthPartitioning") {
    var dataUrls = HourlyTrmm.generateTrmmDaily(1999)
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
    var dataUrls = HourlyTrmm.generateTrmmDaily(1999,2000)
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