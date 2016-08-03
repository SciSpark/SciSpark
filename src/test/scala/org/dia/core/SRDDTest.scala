/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dia.core

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.dia.Constants._
import org.dia.loaders.NetCDFReader._
import org.dia.loaders.TestMatrixReader._
import org.dia.partitioners.SPartitioner._
import org.dia.partitioners.STrmmPartitioner._
import org.dia.testenv.SparkTestConstants
import org.dia.urlgenerators.HourlyTrmmURLGenerator
import org.dia.utils.NetCDFUtils

/**
 * Tests for proper construction of SRDD's from URIs.
 */
class SRDDTest extends FunSuite with BeforeAndAfter {

  val gesdiscUrl = "http://disc2.gesdisc.eosdis.nasa.gov:80"
  before {
    NetCDFUtils.setHTTPAuthentication(gesdiscUrl, "scispark", "SciSpark1")
  }

  val testLinks = SparkTestConstants.datasetPath

  val fakeURI = List("0000010100")
  val varName = List("randVar", "randVar_1")
  val sc = SparkTestConstants.sc.sparkContext
  sc.setLocalProperty(ARRAY_LIB, BREEZE_LIB)
  // Nd4j has issues with kryo serialization
  val sRDD = new SRDD[SciTensor](sc, fakeURI, varName, loadTestArray, mapOneUrl)
  val fakeURIs = List("0000010100", "0000010101", "0000010102", "0000010103", "0000010104", "0000010105")
  val uSRDD = new SRDD[SciTensor](sc, fakeURIs, varName, loadTestUniformArray, mapOneUrl)

  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  test("filterGreaterThan") {
    logger.info("In filterGreaterThan test ...")
    val t = sRDD.map(p => p("randVar") > 241.0).collect().toList
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    val tVals = t(0).variables.values.toList(0).data
    val count = tVals.count(p => p != 0.0)
    logger.info(count + " are greater than 241.0")

    assert(count == 8)
  }

  test("filterGreaterThanEquals") {
    logger.info("In filterGreaterThanEquals test ...")
    val t = sRDD.map(p => p("randVar") >= 241.0).collect.toList
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    val tVals = t(0).variables.values.toList(0).data
    val count = tVals.count(p => p != 0.0)
    logger.info(count + " are greater than or equals to 241.0")

    assert(count == 18)
  }

  test("filterEquals") {
    logger.info("In filterLessThan test ...")
    val t = sRDD.map(p => p("randVar") := 241.0).collect.toList
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    val tVals = t(0).variables.values.toList(0).data
    val count = tVals.count(p => p != 0.0)
    logger.info(count + " are equal to 241.0")

    assert(count == 10)
  }

  test("filterNotEquals") {
    logger.info("In filterNotEquals test ...")
    val t = sRDD.map(p => p("randVar") != 241.0).collect.toList
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    val tVals = t(0).variables.values.toList(0).data
    val count = tVals.count(p => p != 0.0)
    logger.info(count + " are not equals to 241.0")

    assert(count == 20)
  }

  test("filterLessThan") {
    logger.info("In filterLessThan test ...")
    val t = sRDD.map(p => p("randVar") < 241.0).collect.toList
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    val tVals = t(0).variables.values.toList(0).data
    val count = tVals.count(p => p != 0.0)
    logger.info(count + " are less than 241.0")

    assert(count == 12)
  }

  test("filterLessThanEquals") {
    logger.info("In filterLessThanEquals test ...")
    val t = sRDD.map(p => p("randVar") <= 241.0).collect.toList
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    val tVals = t(0).variables.values.toList(0).data
    val count = tVals.count(p => p != 0.0)
    logger.info(count + " are less than or equals to 241.0")

    assert(count == 22)
  }

  /**
    * Test matrix artihmetic
    **/
  test("inplacematrixAddition") {
    logger.info("In inPlaceMatrixAddition test ...")
    logger.info("The sciTensor is: " + uSRDD.collect().toList)
    logger.info("The values are: " + uSRDD.collect().toList(0).variables.values.toList)
    // test scalar addition
    val s = uSRDD.map(p => p("randVar") + 2.0).collect().toList
    logger.info("Addition: " + s(0).variables.values.toList(0).toString.split("  ").toList.filter(_ != ""))
    val sVals = s(0).variables.values.toList(0).data
    val count = sVals.count(p => p != 3.0)
    assert(count == 0)
  }

  test("outofplaceMatrixAddition") {
    // test scalar addition
    val t = uSRDD.map(p => p("randVar") :+ 2.0).collect.toList
    logger.info("Addition: " + t(0).variables.values.toList(0).toString.split("  ").toList.filter(_ != ""))
    val y1 = t(0).variables.values.toList(0).data
    val count = y1.count(p => p != 3.0)
    assert(count == 0)
  }

  test("orderedPairWiseInplaceMatrixAddition") {
    // test matrix addition
    // order the fakeURLs then sum pairs
    val ordered = uSRDD.flatMap(p => {
      List((p.metaData("SOURCE").toInt, p), (p.metaData("SOURCE").toInt + 1, p))
    }).groupBy(_._1)
      .map(p => p._2.map(e => e._2).toList)
      .filter(p => p.size > 1)
      .map(p => p.sortBy(_.metaData("SOURCE").toInt))
      .map(p => (p(0), p(1)))

    val c = ordered.collect.toList
    for (i <- c) logger.info(i._1.metaData + " connected to " + i._2.metaData)

    val ew = ordered.map(p => p._1("randVar") + p._2("randVar"))
    val ewList = ew.collect().toList
    logger.info("List length before is: " + c.length + " and after is length " + ewList.length)
    for (sciT <- ewList) {
      assert(sciT.variables.values.toList(0).toString.split("  ").toList.filter(_ != "").filter(_ != 2.0).length == 30)
    }
  }

  test("orderedPairWiseOutofplaceMatrixAddition") {
    val ordered = uSRDD.flatMap(p => {
      List((p.metaData("SOURCE").toInt, p), (p.metaData("SOURCE").toInt + 1, p))
    }).groupBy(_._1)
      .map(p => p._2.map(e => e._2).toList)
      .filter(p => p.size > 1)
      .map(p => p.sortBy(_.metaData("SOURCE").toInt))
      .map(p => (p(0), p(1)))

    val c = ordered.collect.toList

    val ew1 = ordered.map(p => p._1("randVar") :+ p._2("randVar"))
    val ewList1 = ew1.collect().toList
    logger.info("List length before is: " + c.length + " and after is length " + ewList1.length)
    for (eachArray <- ewList1) {
      assert(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_ != "").filter(_ != 2.0).length == 30)
    }
  }

  test("inPlaceMatrixSubtraction") {
    val t = uSRDD.collect.toList
    logger.info("The sciTensor is: " + t.toList)
    logger.info("The values are: " + t(0).variables.values.toList)
    // test scalar subtraction
    val s = uSRDD.map(p => p("randVar") - 2.0).collect.toList
    logger.info("Subtraction: " + s(0).variables.values.toList(0).toString.split("  ").toList.filter(_ != ""))
    val y = s(0).variables.values.toList(0).data
    val count = y.count(p => p != -1.0)
    assert(count == 0)
  }

  test("OutofplaceMatrixSubtraction") {
    // test scalar subtraction
    val t = uSRDD.map(p => p("randVar") :- 2.0).collect.toList
    val y1 = t(0).variables.values.toList(0).data
    val count = y1.count(p => p != -1.0)
    assert(count == 0)
  }

  test("orderedPairWiseOutofplaceMatrixSubtraction") {
    // test matrix subtraction
    // order the fakeURLs then subtract pairs
    val ordered = uSRDD.flatMap(p => {
      List((p.metaData("SOURCE").toInt, p), (p.metaData("SOURCE").toInt + 1, p))
    }).groupBy(_._1)
      .map(p => p._2.map(e => e._2).toList)
      .filter(p => p.size > 1)
      .map(p => p.sortBy(_.metaData("SOURCE").toInt))
      .map(p => (p(0), p(1)))
    val c = ordered.collect.toList
    for (i <- c) logger.info(i._1.metaData + " connected to " + i._2.metaData)

    val ew = ordered.map(p => p._2("randVar") - p._1("randVar"))
    val ewList = ew.collect().toList
    logger.info("List length before is: " + c.length + " and after is length " + ewList.length)
    for (sciT <- ewList) {
      assert(sciT.variables.values.toList(0).toString.split("  ").toList.filter(_ != "").filter(_ != 0.0).length == 30)
    }
  }

  test("orderedPairWiseInplaceMatrixSubtraction") {
    val ordered = uSRDD.flatMap(p => {
      List((p.metaData("SOURCE").toInt, p), (p.metaData("SOURCE").toInt + 1, p))
    }).groupBy(_._1)
      .map(p => p._2.map(e => e._2).toList)
      .filter(p => p.size > 1)
      .map(p => p.sortBy(_.metaData("SOURCE").toInt))
      .map(p => (p(0), p(1)))
    val c = ordered.collect.toList

    val ew1 = ordered.map(p => p._2("randVar") :- p._1("randVar"))
    val ewList1 = ew1.collect().toList
    logger.info("List length before is: " + c.length + " and after is length " + ewList1.length)
    for (sciT <- ewList1) {
      if (sciT.variables.values.toList(0).toString.split("  ").toList.filter(_ != "").filter(_ != 0.0).length == 30) {
        assert(true)
      } else {
        assert(false)
      }
    }
  }

  test("matrixDivision") {
    logger.info("In matrixDivision test ...")
    logger.info("The sciTensor is: " + uSRDD.collect().toList)
    logger.info("The values are: " + uSRDD.collect().toList(0).variables.values.toList)
    // test scalar division
    val s = uSRDD.map(p => p("randVar") / 2.0).collect().toList(0)
    logger.info("Division: " + s.variables.values.toList(0).toString.split("  ").toList.filter(_ != ""))
    val y = s.variables.values.toList(0).toString.split("  ").toList.filter(_ != "")
    if (y.filter(_.toDouble != 0.5).length == 0) {
      assert(true)
    } else {
      assert(false)
    }
    val t = uSRDD.map(p => p("randVar") :/ 2.0).collect.toList(0)
    logger.info("Division: " + t.variables.values.toList(0).toString.split("  ").toList.filter(_ != ""))
    val y1 = t.variables.values.toList(0).toString.split("  ").toList.filter(_ != "")
    if (y1.filter(_.toDouble != 0.5).length == 0) {
      assert(true)
    } else {
      assert(false)
    }
    // test matrix division
    // order the fakeURLs then multiply pairs
    val ordered = uSRDD.flatMap(p => {
      List((p.metaData("SOURCE").toInt, p), (p.metaData("SOURCE").toInt + 1, p))
    }).groupBy(_._1)
      .map(p => p._2.map(e => e._2).toList)
      .filter(p => p.size > 1)
      .map(p => p.sortBy(_.metaData("SOURCE").toInt))
      .map(p => (p(0), p(1)))
    val c = ordered.collect.toList
    for (i <- c) logger.info(i._1.metaData + " connected to " + i._2.metaData)

    val ew = ordered.map(p => p._1("randVar") :/ p._2("randVar"))
    val ewList = ew.collect().toList
    logger.info("List length before is: " + c.length + " and after is length " + ewList.length)
    for (eachArray <- ewList) {
      if (eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_ != "").filter(_ != 1.0).length == 30) {
        assert(true)
      } else {
        assert(false)
      }
    }

    val ew1 = ordered.map(p => p._1("randVar") / p._2("randVar"))
    val ewList1 = ew1.collect().toList
    logger.info("List length before is: " + c.length + " and after is length " + ewList1.length)
    for (eachArray <- ewList1) {
      if (eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_ != "").filter(_ != 1.0).length == 30) {
        assert(true)
      } else {
        assert(false)
      }
    }
  }

  test("matrixMultiplication") {
    logger.info("In matrixMultiplication test ...")
    logger.info("The sciTensor is: " + uSRDD.collect().toList)
    logger.info("The values are: " + uSRDD.collect().toList(0).variables.values.toList)
    // test scalar multiplication
    val s = uSRDD.map(p => p("randVar") * 2.0).collect.toList(0)
    logger.info("Multiplication: " + s.variables.values.toList(0).toString.split("  ").toList.filter(_ != ""))
    val y = s.variables.values.toList(0).toString.split("  ").toList.filter(_ != "")
    if (y.filter(_.toDouble != 2.0).length == 0) {
      assert(true)
    } else {
      assert(false)
    }
    // test scalar multiplication
    val t = uSRDD.map(p => p("randVar") :* 2.0)
    val y1 = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_ != "")
    if (y1.filter(_.toDouble != 2.0).length == 0) {
      assert(true)
    } else {
      assert(false)
    }
    // test matrix multiplication
    // order the fakeURLs then multiply pairs
    val ordered = uSRDD.flatMap(p => {
      List((p.metaData("SOURCE").toInt, p), (p.metaData("SOURCE").toInt + 1, p))
    }).groupBy(_._1)
      .map(p => p._2.map(e => e._2).toList)
      .filter(p => p.size > 1)
      .map(p => p.sortBy(_.metaData("SOURCE").toInt))
      .map(p => (p(0), p(1)))
    val c = ordered.collect.toList
    for (i <- c) logger.info(i._1.metaData + " connected to " + i._2.metaData)
    val ew = ordered.map(p => p._1("randVar") * p._2("randVar"))
    val ewList = ew.collect().toList
    for (eachArray <- ewList) {
      if (eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_ != "").filter(_ != 1.0).length == 30) {
        assert(true)
      } else {
        assert(false)
      }
    }

    val ew1 = ordered.map(p => p._1("randVar") :* p._2("randVar"))
    val ewList1 = ew.collect().toList
    logger.info("List length before is: " + c.length + " and after is length " + ewList1.length)
    for (eachArray <- ewList1) {
      if (eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_ != "").filter(_ != 1.0).length == 30) {
        assert(true)
      } else {
        assert(false)
      }
    }
  }

  /**
   * Test data retrieval - values,shape
   **/
  test("getVarData") {
    println("getVarData test ...")
    val x = Array(240.0, 241.0, 240.0, 241.0, 241.0, 230.0, 231.0, 240.0, 222.0, 241.0, 242.0, 243.0, 244.0, 241.0, 232.0, 240.0,
      241.0, 230.0, 231.0, 241.0, 240.0, 241.0, 240.0, 242.0, 241.0, 242.0, 243.0, 244.0, 241.0, 242.0)
    val t = sRDD.map(p => (p("randVar").data, p("randVar").shape))
    val varData = t.collect()(0)
    logger.info("The data is: " + varData._1.mkString(" ") + "\nShape: (" + varData._2.mkString(" , ") + ")")
    if (varData._2(0) == 6 && varData._2(1) == 5 && varData._1.length == 30 && varData._1.sameElements(x)) {
      assert(true)
    } else {
      assert(false)
    }
  }

  /**
   * Test varInUse
   **/
  test("varInUse") {
    println("varInUse test ...")
    val t = sRDD.map(p => (p("randVar").data, p("randVar").shape))
    val varData = t.collect()(0)
    logger.info("The randVar data is: " + varData._1.mkString(" ") + "\nShape: (" + varData._2.mkString(" , ") + ")")

    val t1 = sRDD.map(p => (p("randVar_1").data, p("randVar_1").shape))
    val varData1 = t1.collect()(0)
    logger.info("The randVar_1 data is: " + varData1._1.mkString(" ") + "\nShape: (" + varData1._2.mkString(" , ") + ")")

    val tdefault = sRDD.map(p => (p.data, p.shape))
    val varDataDef = tdefault.collect()(0)
    logger.info("The varInUse default data is: " +
      varDataDef._1.mkString(" ") +
      "\nShape: (" +
      varDataDef._2.mkString(" , ") + ")")

    if (!(varData._1.sameElements(varData1._1)) && (varData1._1.sameElements(varDataDef._1))) {
      assert(true)
    } else {
      assert(false)
    }
  }

  test("GroupingByYearPartitioning") {
    val urls = HourlyTrmmURLGenerator.generateTrmmDaily(1999, 2000).toList.slice(0, 2)
    sc.setLocalProperty("log4j.configuration", "resources/log4j-defaults.properties")
    val sRDD = new SRDD[SciTensor](sc, urls, List("precipitation"), loadNetCDFNDVar, mapOneYearToManyTensorTRMM)
    val Tensor = sRDD.collect()(0)
    assert(true)
  }

  test("GroupingByDayPartitioning") {
    val urls = HourlyTrmmURLGenerator.generateTrmmDaily(1999, 2000).toList.slice(0, 2)
    sc.setLocalProperty("log4j.configuration", "resources/log4j-defaults.properties")
    val sRDD = new SRDD[SciTensor](sc, urls, List("precipitation"), loadNetCDFNDVar, mapOneDayToManyTensorTRMM)
    val Tensor = sRDD.collect()(0)
    assert(true)
  }

}
