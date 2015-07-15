package org.dia.core

import java.util

import _root_.breeze.linalg.DenseMatrix
import org.dia.TRMMUtils.{HourlyTrmm, OpenDapTRMMURLGenerator}
import org.dia.core.singleUrl.{sciBreezeRDD, sciNd4jRDD}
import org.joda.time.DateTime
import org.nd4j.linalg.api.ndarray.INDArray
import org.scalatest.FunSuite;

import scala.collection.generic.MutableMapFactory
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, HashMap}
import scala.io.Source

/**
 * Tests for creating different Rdd types.
 * Created by marroquin on 7/14/15.
 */
class sRddTest extends FunSuite  {
  test("SimplePartitionScheme") {
    val dataUrls = Source.fromFile("TestLinks").mkString.split("\n").toList
//    println(dataUrls.toMap[String, String])
    //and now we will construct a map using the list of settings
    var cnt = -1
    val dataMapping = dataUrls.map(elem => {cnt+=1; (cnt, elem);}).toMap
    //the above codes are similar to the first example, except we using the map function to convert the settings list into a list of tuple.

    println(dataMapping)
    assert(true)
//    val datasetMapping: Map[AnyVal, Any] = datasetUrls.foreach(element => (0, element)).asInstanceOf[Map[AnyVal, Any]]
//    datasetMapping.keys.foreach(k => println(datasetMapping.get(k)))
  }

  test("GroupingByDayPartitioning") {
    val dataMapping = HourlyTrmm.generateTrmmDaily(1999)
//    for((key, value) <- dataMapping) {
//      println(key.toString("yyyy-MM-dd") + "=>" + value)
//    }
    val sc = SparkTestConstants.sc
    val sRdd = new sciBreezeRDD[HashMap[String, DenseMatrix[Double]]] (sc, dataMapping, Groupers.mapDayUrls, "precipitation")
    println("]]]]]]]")
    println(sRdd.collect().length)
    println("]]]]]]]")
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
    val sciContext = SparkTestConstants.sc
    val srdd = sciContext.OpenDapURLFile("TestLinks", "TotCldLiqH2O_A")

    val collected = srdd.collect
    collected.map(p => println(p))
    assert(true)
  }
}