package org.dia.sLib

import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.dia.TRMMUtils.Parsers
import org.dia.core.{sciTensor, sRDD, SparkTestConstants}
import org.dia.tensors.Nd4jTensor
import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite
import org.nd4j.api.Implicits._

import java.math

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.io.Source

/**
 * Created by rahulsp on 8/4/15.
 */
class mccOps$Test extends FunSuite {

  test("testFindConnectedComponents") {
    val m = Array(
      Array(1.0, 2.0, 0.0, 4.0),
      Array(5.0, 6.0, 0.0, 8.0),
      Array(43.9, 23.0, 1.0,0.0),
      Array(0.0,0.0,0.0,0.0),
      Array(1.0, 0.0, 1.0, 0.0)
    )

    val ndArray = Nd4j.create(m)
    val t = new Nd4jTensor(ndArray)
    val labelled = mccOps.labelConnectedComponents(t)
    //println(labelled)
    assert(true)
  }

  test("findComponents") {
    val m = Array(
      Array(1.0, 2.0, 0.0, 4.0),
      Array(5.0, 6.0, 0.0, 8.0),
      Array(43.9, 23.0, 1.0,0.0),
      Array(0.0,0.0,0.0,0.0),
      Array(1.0, 0.0, 1.0, 0.0)
    )

    val ndArray = Nd4j.create(m)
    val t = new Nd4jTensor(ndArray)
    val frames = mccOps.findCloudElements(t)
    frames.map(p => {
      println(p)
      println(mccOps.areaFilled(p))
    })

    assert(true)
  }

  test("testBACKGROUND") {
    val m = Array(
      Array(1.0, 2.0, 0.0, 4.0),
      Array(5.0, 6.0, 0.0, 8.0),
      Array(43.9, 23.0, 1.0,0.0),
      Array(0.0,0.0,0.0,0.0),
      Array(1.0, 0.0, 1.0, 0.0)
    )

    val ndArray = Nd4j.create(m)
    println(ndArray)
    val t = ndArray.map(p => if(p <= 23.0) p else 0.0)
    println(t)
    assert(true)
  }

  test("testReduceResolution") {

  }

  test("MCC") {
    val variable = "randomVar"
    val rdd = SparkTestConstants.sc.NetcdfFile("TestLinks2", List("randomVar"))
    val filtered = rdd.map(p => p(variable) <= 241)

    val componentFrameRDD = filtered.flatMap(p => mccOps.findCloudElements(p))

    val criteriaRDD = componentFrameRDD.filter(p => {
      val hash = p.metaData
      val area = hash("AREA").toDouble
      val tempDiff = hash("DIFFERENCE").toDouble
      (area >= 9.0) || (area < 4.0) && (tempDiff > 10.0)
    })


    val dates = Source.fromFile("TestLinks2").mkString.split("\n").toList.map(p => p.replaceAllLiterally(".", "/")).map(p => Parsers.ParseDateFromString(p))

    val vertexSet = getVertexArray(criteriaRDD)

    val dateMappedRDDs = dates.map(p => {
      val compareString = new SimpleDateFormat("yyyy-MM-dd").format(p)
      (p, criteriaRDD.filter(_.metaData("FRAME") == compareString))
    })
    var edgeRDD : RDD[(Long, Long)] = null
    //var preEdgeAccumulator: Accumulator[List[(Long, Long)]] = sc.sparkContext.accumulator(List((0L, 0L)), "EdgeAccumulation")(EdgeAccumulator)
    for (index <- 0 to dateMappedRDDs.size - 2) {
      val currentTimeRDD = dateMappedRDDs(index)._2
      val nextTimeRDD = dateMappedRDDs(index + 1)._2
      //      val currCount = currentTimeRDD.count
      //      val nextCount = nextTimeRDD.count
      val cartesianPair = currentTimeRDD.cartesian(nextTimeRDD)
      val findEdges = cartesianPair.filter(p => (p._1.tensor * p._2.tensor).isZero == false)
      val edgePair = findEdges.map(p => (vertexSet(p._1.metaData("FRAME") + p._1.metaData("COMPONENT")), vertexSet(p._2.metaData("FRAME") + p._2.metaData("COMPONENT"))))
      if(edgeRDD == null) {
        edgeRDD = edgePair
      } else {
        edgeRDD = edgeRDD ++ edgePair
      }
    }

    val collectedEdges = edgeRDD.collect

    vertexSet.map(p => println(p))
    collectedEdges.map(p => println(p))
    println(collectedEdges.size)
    dates.map(p => println(p))
    val collect = rdd.collect

    collect.toList.map(p => println(p.variables(p.varInUse) + "\n"))
    assert(true)
  }

  def getVertexArray(collection: sRDD[sciTensor]): HashMap[String, Long] = {
    val id = collection.map(p => p.metaData("FRAME") + p.metaData("COMPONENT")).collect.toList
    val size = id.length
    val range = 0 to (size - 1)
    val hash = new mutable.HashMap[String, Long]
    range.map(p => hash += ((id(p), p)))
    hash
  }

}
