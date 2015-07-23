package org.dia.core

import breeze.linalg.DenseMatrix
import org.apache.spark.rdd.RDD
import org.dia.tensors.{BreezeTensor, Nd4jTensor}
import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite
import org.dia.Constants._
import org.nd4j.api.Implicits._
/**
 * Created by rahulsp on 7/20/15.
 */
class MCCAlgorithmTest extends FunSuite {

  test("mappedReduceResolutionTest") {
    val sc = SparkTestConstants.sc
    sc.setLocalProperty(ARRAY_LIB, ND4J_LIB)
    val nd4jRDD = sc.OpenDapURLFile("TestLinks2", "TotCldLiqH2O_A")
    val collect = nd4jRDD.map(p => p.reduceResolution(5)).collect

    sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)
    val breezeRDD = sc.OpenDapURLFile("TestLinks2", "TotCldLiqH2O_A")
    val breezeCollect = breezeRDD.map(p => p.reduceResolution(5)).collect


    val underlying = collect(0).tensor.data.toList
    val breezeData = breezeCollect(0).tensor.data.toList
    println(underlying)
    println(breezeData)
    var i = 0
    var numInaccurate = 0
    while(i < underlying.length) {
      if((Math.abs(underlying(i) - breezeData(i)) / underlying(i)) > 0.000001){
        println(i + ": Underlying " + underlying(i) + " Breeze " + breezeData(i))
        numInaccurate += 1
      }
      i += 1
    }
    assert(numInaccurate == 0)
  }

  test("sampleApiTest") {
    val sc : SciSparkContext = SparkTestConstants.sc

    sc.setLocalProperty(ARRAY_LIB, ND4J_LIB)

    val nd4jRDD : sRDD[sciTensor] = sc.OpenDapURLFile("TestLinks", "TotCldLiqH2O_A")

    val smoothRDD : RDD[sciTensor] = nd4jRDD.map(p => p.reduceResolution(5))

    val collect : Array[sciTensor] = smoothRDD.map(p => p <= 241.0).collect

    println(collect.toList)

    assert(true)
  }

  test("reduceResolutionTest") {
    val dense = new DenseMatrix[Double](180, 360, 1d to 64800d by 1d toArray, 0, 360, true)
    val nd = Nd4j.create(1d to 64800d by 1d toArray, Array(180,360))
    val breeze = new BreezeTensor(dense)
    val nd4j = new Nd4jTensor(nd)
//    println(breeze)

//    println("rows" + dense(0, ::))
//    println("rows" + nd.getRow(0))

    val b = breeze.reduceResolution(90)
    val n = nd4j.reduceResolution(90)

    println(b)
    println(n)

    println(b.data.toList)
    println(n.data.toList)

//    println(breeze.data.toList)
//    println(nd4j.data.toList)
  }
}
