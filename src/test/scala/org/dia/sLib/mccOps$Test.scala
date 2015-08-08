package org.dia.sLib

import java.text.SimpleDateFormat

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.dia.TRMMUtils.Parsers
import org.dia.core.{SparkTestConstants, sRDD, sciTensor}
import org.dia.tensors.Nd4jTensor
import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.io.Source


class mccOps$Test extends FunSuite {

  test("testFindConnectedComponents") {
    val m = Array(
      Array(1.0, 2.0, 0.0, 4.0),
      Array(5.0, 6.0, 0.0, 8.0),
      Array(43.9, 23.0, 1.0,0.0),
      Array(0.0,0.0,0.0,0.0),
      Array(1.0, 0.0, 1.0, 0.0)
    )

    val cc = Array(
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 1.0, 0.0),
      Array(0.0, 0.0, 0.0, 0.0),
      Array(3.0, 0.0, 4.0, 0.0)
    )

    val ndArray = Nd4j.create(m)
    val ccArray = Nd4j.create(cc)
    val t = new Nd4jTensor(ndArray)
    val cct = new Nd4jTensor(ccArray)
    val labelled = mccOps.labelConnectedComponents(t)

    assert(labelled._1.equals(cct))
  }

  test("findComponents") {
    val m = Array(
      Array(1.0, 2.0, 0.0, 4.0),
      Array(5.0, 6.0, 0.0, 8.0),
      Array(43.9, 23.0, 1.0,0.0),
      Array(0.0,0.0,0.0,0.0),
      Array(1.0, 0.0, 1.0, 0.0)
    )

    val cc = Array(
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 0.0, 2.0),
      Array(1.0, 1.0, 1.0, 0.0),
      Array(0.0,0.0,0.0,0.0),
      Array(3.0, 0.0, 4.0, 0.0)
    )

    val ndArray = Nd4j.create(m)
    val ccArray = Nd4j.create(cc)
    val t = new Nd4jTensor(ndArray)
    val cct = new Nd4jTensor(ccArray)
    val frames = mccOps.findConnectedComponents(t)
    val ccframes = mccOps.findConnectedComponents(cct)
    assert(frames == ccframes)
  }

  ignore("MCC") {
    LogManager.getLogger(Class.forName("com.joestelmach.natty.Parser")).setLevel(org.apache.log4j.Level.OFF)
    val variable = "randomVar"
    val file = "TestLinks"
    val rdd = SparkTestConstants.sc.NetcdfFile(file, List("randomVar"), 1)
    val filtered = rdd.map(p => p(variable) <= 241)

    val componentFrameRDD = filtered.flatMap(p => mccOps.findConnectedComponents(p))

    val criteriaRDD = componentFrameRDD.filter(p => {
      val hash = p.metaData
      val area = hash("AREA").toDouble
      val tempDiff = hash("DIFFERENCE").toDouble
      (area >= 9.0) || (area < 4.0) && (tempDiff > 10.0)
    })


    val dates = Source.fromFile(file).mkString.split("\n").toList.map(p => p.split("/").last.replaceAllLiterally(".", "/")).map(p => Parsers.ParseDateFromString(p)).sorted

    val vertexSet = getVertexArray(criteriaRDD)

    val dateMappedRDDs = dates.map(p => {
      val compareString = new SimpleDateFormat("yyyy-MM-dd").format(p)
      (p, criteriaRDD.filter(_.metaData("FRAME") == compareString))
    })
    var edgeRDD : RDD[(Long, Long)] = null
    //var preEdgeAccumulator: Accumulator[List[(Long, Long)]] = sc.sparkContext.accumulator(List((0L, 0L)), "EdgeAccumulation")(EdgeAccumulator)
    for (index <- 0 to dateMappedRDDs.size - 2) {
      println(index)
      val currentTimeRDD = dateMappedRDDs(index)._2
      val nextTimeRDD = dateMappedRDDs(index + 1)._2
      val cartesianPair = currentTimeRDD.cartesian(nextTimeRDD)
      val findEdges = cartesianPair.filter(p => !(p._1.tensor * p._2.tensor).isZero)
      val edgePair = findEdges.map(p => (vertexSet(p._1.metaData("FRAME") + p._1.metaData("COMPONENT")), vertexSet(p._2.metaData("FRAME") + p._2.metaData("COMPONENT"))))
      if(edgeRDD == null) {
        edgeRDD = edgePair
      } else {
        edgeRDD = edgeRDD ++ edgePair
      }
    }

    val collectedEdges = edgeRDD.collect()

    println("The vertex set : ")
    vertexSet.toList.sortBy(p => p._2).foreach(p => println(p))
    println("There are " + collectedEdges.length + " edges in the edge set : ")
    collectedEdges.foreach(p => println(p))
    dates.foreach(p => println(p))


    //collect.toList.map(p => println(p.variables(p.varInUse) + "\n"))
    assert(true)
  }

  def getVertexArray(collection: sRDD[sciTensor]): mutable.HashMap[String, Long] = {
    val id = collection.map(p => p.metaData("FRAME") + p.metaData("COMPONENT")).collect().toList
    val size = id.length
    val range = 0 to (size - 1)
    val hash = new mutable.HashMap[String, Long]
    range.map(p => hash += ((id(p), p)))
    hash
  }

}
