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

import org.dia.loaders.NetCDFReader._
import org.dia.loaders.TestMatrixReader._
import org.dia.partitioners.SPartitioner._
import org.dia.partitioners.STrmmPartitioner._
import org.dia.tensors.Nd4jTensor
import org.dia.testenv.SparkTestConstants
import org.dia.urlgenerators.HourlyTrmmURLGenerator
import org.dia.utils.NetCDFUtils

/**
 *
 * Tests for proper construction of SRDD's from URIs.
 */
class SRDDTest extends FunSuite with BeforeAndAfter {

  val gesdiscUrl = "http://disc2.gesdisc.eosdis.nasa.gov:80"
  val username = SparkTestConstants.testHTTPCredentials("username")
  val password = SparkTestConstants.testHTTPCredentials("password")
  before {
    NetCDFUtils.setHTTPAuthentication(gesdiscUrl, username, password)
  }

  val testLinks = SparkTestConstants.datasetPath

  val fakeURI = List("0000010100")
  val varName = List("randVar", "randVar_1")
  val fakeURIs = List("0000010100", "0000010101", "0000010102", "0000010103", "0000010104", "0000010105")
  val sc = SparkTestConstants.sc.sparkContext
  val sRDD = new SRDD[SciTensor](sc, fakeURI, varName, loadTestArray, mapOneUrl)
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
    val t = sRDD.map(p => p("randVar") >= 241.0).collect().toList
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    val tVals = t(0).variables.values.head.data
    val count = tVals.count(p => p != 0.0)
    logger.info(count + " are greater than or equals to 241.0")

    assert(count == 18)
  }

  test("filterEquals") {
    logger.info("In filterLessThan test ...")
    val t = sRDD.map(p => p("randVar") := 241.0).collect().toList
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    val tVals = t(0).variables.values.head.data
    val count = tVals.count(p => p != 0.0)
    logger.info(count + " are equal to 241.0")

    assert(count == 10)
  }

  test("filterNotEquals") {
    logger.info("In filterNotEquals test ...")
    val t = sRDD.map(p => p("randVar") != 241.0).collect().toList
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    val tVals = t(0).variables.values.head.data
    val count = tVals.count(p => p != 0.0)
    logger.info(count + " are not equals to 241.0")

    assert(count == 20)
  }

  test("filterLessThan") {
    logger.info("In filterLessThan test ...")
    val t = sRDD.map(p => p("randVar") < 241.0).collect().toList
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    val tVals = t(0).variables.values.head.data
    val count = tVals.count(p => p != 0.0)
    logger.info(count + " are less than 241.0")

    assert(count == 12)
  }

  test("filterLessThanEquals") {
    logger.info("In filterLessThanEquals test ...")
    val t = sRDD.map(p => p("randVar") <= 241.0).collect().toList
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    val tVals = t(0).variables.values.head.data
    val count = tVals.count(p => p != 0.0)
    logger.info(count + " are less than or equals to 241.0")

    assert(count == 22)
  }

  /**
   * Test matrix artihmetic
   */
  test("outofplacematrixAddition") {
    val t = uSRDD.collect().toList
    logger.info("In outofPlaceMatrixAddition test ...")
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    // test scalar addition
    val s = uSRDD.map(p => p("randVar") + 2.0).collect().toList
    logger.info("Addition: " + s(0).variables.values.head)
    val sVals = s(0).variables.values.head.data
    val count = sVals.count(p => p != 3.0)
    assert(count == 0)
  }

  test("inplaceMatrixAddition") {
    // test scalar addition
    val t = uSRDD.map(p => p("randVar") += 2.0).collect().toList
    logger.info("Addition: " + t(0).variables.values.head)
    val y1 = t(0).variables.values.head.data
    val count = y1.count(p => p != 3.0)
    assert(count == 0)
  }

  test("orderedPairWiseOutplaceMatrixAddition") {
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
    val clen = c.length
    for (i <- c) logger.info(i._1.metaData + " connected to " + i._2.metaData)

    val ew = ordered.map(p => p._1("randVar") + p._2("randVar"))
    val ewList = ew.collect().toList
    val ewLen = ewList.length
    logger.info("List length before is: " + clen + " and after is length " + ewLen)
    for (sciT <- ewList) {
      assert(sciT.variables.values.head.data.count(_ == 2.0) == 30)
    }
  }

  test("orderedPairWiseInplaceMatrixAddition") {
    val ordered = uSRDD.flatMap(p => {
      List((p.metaData("SOURCE").toInt, p), (p.metaData("SOURCE").toInt + 1, p))
    }).groupBy(_._1)
      .map(p => p._2.map(e => e._2).toList)
      .filter(p => p.size > 1)
      .map(p => p.sortBy(_.metaData("SOURCE").toInt))
      .map(p => (p(0), p(1)))

    val c = ordered.collect.toList
    val clen = c.length
    val ew1 = ordered.map(p => p._1("randVar") += p._2("randVar"))
    val ewList1 = ew1.collect().toList
    val ewLen = ewList1.length
    logger.info("List length before is: " + clen + " and after is length " + ewLen)
    for (sciT <- ewList1) {
      assert(sciT.variables.values.head.data.count(_ == 2.0) == 30)
    }
  }

  test("OutPlaceMatrixSubtraction") {
    val t = uSRDD.collect().toList
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    // test scalar subtraction
    val s = uSRDD.map(p => p("randVar") - 2.0).collect().toList
    logger.info("Subtraction: " + s(0).variables.values.head)
    val y = s(0).variables.values.head.data
    val count = y.count(p => p != -1.0)
    assert(count == 0)
  }

  test("InofplaceMatrixSubtraction") {
    // test scalar subtraction
    val t = uSRDD.map(p => p("randVar") -= 2.0).collect().toList
    val y1 = t(0).variables.values.head.data
    val count = y1.count(p => p != -1.0)
    assert(count == 0)
  }

  test("orderedPairWiseOutplaceMatrixSubtraction") {
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
    val clen = c.length
    for (i <- c) logger.info(i._1.metaData + " connected to " + i._2.metaData)

    val ew = ordered.map(p => p._2("randVar") - p._1("randVar"))
    val ewList = ew.collect().toList
    val ewLen = ewList.length
    logger.info("List length before is: " + clen + " and after is length " + ewLen)
    for (sciT <- ewList) {
      assert(sciT.variables.values.head.data.count(_ == 0.0) == 30)
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
    val clen = c.length

    val ew1 = ordered.map(p => p._2("randVar") -= p._1("randVar"))
    val ewList1 = ew1.collect().toList
    val ewLen = ewList1.length
    logger.info("List length before is: " + clen + " and after is length " + ewLen)
    for (sciT <- ewList1) {
      assert(sciT.variables.values.head.data.count(_ == 0.0) == 30)
    }
  }

  test("OutPlaceMatrixDivision") {
    val t = uSRDD.collect().toList
    logger.info("In matrixDivision test ...")
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    // test scalar division
    val s = uSRDD.map(p => p("randVar") / 2.0).collect().head
    logger.info("Division: " + s.variables.values.head)
    val y = s.variables.values.head.data
    assert(y.count(_ == 0.5) == 30)
  }

  test("InPlaceMatrixDivision") {
    val t = uSRDD.map(p => p("randVar") /= 2.0).collect().toList(0)
    logger.info("Division: " + t.variables.values.head)
    val y1 = t.variables.values.head.data
    assert(y1.count(_ == 0.5) == 30)
  }

  test("orderedPairWiseInPlaceMatrixDivision") {
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

    val ew = ordered.map(p => p._1("randVar") /= p._2("randVar"))
    val ewList = ew.collect().toList
    val ewlen = ewList.length
    logger.info("List length before is: " + c.length + " and after is length " + ewlen)
    for (sciT <- ewList) {
      assert(sciT.variables.values.head.data.count(_ == 1.0) == 30)
    }
  }

  test("orderedPairWiseOutPlaceMatrixDivision") {
    val ordered = uSRDD.flatMap(p => {
      List((p.metaData("SOURCE").toInt, p), (p.metaData("SOURCE").toInt + 1, p))
    }).groupBy(_._1)
      .map(p => p._2.map(e => e._2).toList)
      .filter(p => p.size > 1)
      .map(p => p.sortBy(_.metaData("SOURCE").toInt))
      .map(p => (p(0), p(1)))
    val c = ordered.collect.toList
    val clen = c.length
    for (i <- c) logger.info(i._1.metaData + " connected to " + i._2.metaData)

    val ew1 = ordered.map(p => p._1("randVar") / p._2("randVar"))
    val ewList1 = ew1.collect().toList
    val ew1len = ewList1.length
    logger.info("List length before is: " + clen + " and after is length " + ew1len)
    for (sciT <- ewList1) {
      assert(sciT.variables.values.head.data.count(_ == 1.0) == 30)
    }
  }

  test("outPlaceMatrixMultiplication") {
    val t = uSRDD.collect().toList
    logger.info("In matrixMultiplication test ...")
    logger.info("The sciTensor is: " + t)
    logger.info("The values are: " + t(0).variables.values.toList)
    // test scalar multiplication
    val s = uSRDD.map(p => p("randVar") * 2.0).collect().toList
    logger.info("Multiplication: " + s(0).variables.values.head)
    val y = s(0).variables.values.head.data
    assert(y.count(_ == 2.0) == 30)
  }

  test("InPlaceMatrixMultiplication") {
    // test scalar multiplication
    val t = uSRDD.map(p => p("randVar") *= 2.0)
    val y1 = t.collect().head.data
    assert(y1.count(_ == 2.0) == 30)
  }

  test("orderedPairWiseOutPlaceMatrixMultiplication") {
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
    for (sciT <- ewList) {
      assert(sciT.variables.values.head.data.count(_ == 1.0) == 30)
    }
  }

  test("orderedPairWiseInPlaceMatrixMultiplication") {
    // test matrix multiplication
    // order the fakeURLs then multiply pairs
    val ordered = uSRDD.flatMap(p =>
      List((p.metaData("SOURCE").toInt, p), (p.metaData("SOURCE").toInt + 1, p))
    ).groupBy(_._1)
      .map(p => p._2.map(e => e._2).toList)
      .filter(p => p.size > 1)
      .map(p => p.sortBy(_.metaData("SOURCE").toInt))
      .map(p => (p(0), p(1)))

    val c = ordered.count()
    val ew1 = ordered.map(p => p._1("randVar") *= p._2("randVar"))
    val ewList1 = ew1.collect().toList
    val ewLen = ewList1.length
    logger.info("List length before is: " + c + " and after is length " + ewLen)
    for (sciT <- ewList1) {
      assert(sciT.variables.values.head.data.count(_ == 1.0) == 30)
    }
  }

  /**
   *
   * Test data retrieval - values,shape
   */
  test("getVarData") {
    val x = Array(240.0, 241.0, 240.0, 241.0, 241.0, 230.0,
                  231.0, 240.0, 222.0, 241.0, 242.0, 243.0,
                  244.0, 241.0, 232.0, 240.0, 241.0, 230.0,
                  231.0, 241.0, 240.0, 241.0, 240.0, 242.0,
                  241.0, 242.0, 243.0, 244.0, 241.0, 242.0)

    val assertionTensor = new Nd4jTensor(x, Array(6, 5))
    val t = sRDD.map(p => p("randVar").tensor).collect()
    val varData = t(0)
    logger.info("The data is: " + varData + "\nShape: " + varData.shape.toList + "\n")
    assert(assertionTensor == varData)
  }

  /**
   *
   * Test varInUse
   */
  test("varInUse") {
    val t = sRDD.map(p => p("randVar").tensor).collect()
    logger.info("The randVar data is: " + t(0) + "\nShape: " + t(0).shape.toList + "\n")

    val t1 = sRDD.map(p => p("randVar_1").tensor).collect()
    logger.info("The randVar_1 data is: " + t1(0) + "\nShape: " + t1(0).shape.toList + "\n")

    val td = sRDD.map(p => p.tensor).collect()
    logger.info("The default data is: " + td(0) + "\nShape: " + td(0).shape.toList + "\n")

    assert(t(0) != t1(0))
    assert(t1(0) == td(0))
  }

  test("GroupingByYearPartitioning") {
    val urls = HourlyTrmmURLGenerator.generateTrmmDaily(1999, 2000).toList.slice(0, 2)
    sc.setLocalProperty("log4j.configuration", "resources/log4j-defaults.properties")
    val varList = List("precipitation")
    val sRDD = new SRDD[SciTensor](sc, urls, varList, loadNetCDFNDVar, mapOneYearToManyTensorTRMM)
    val Tensor = sRDD.collect()(0)
    assert(true)
  }

  test("GroupingByDayPartitioning") {
    val urls = HourlyTrmmURLGenerator.generateTrmmDaily(1999, 2000).toList.slice(0, 2)
    sc.setLocalProperty("log4j.configuration", "resources/log4j-defaults.properties")
    val varList = List("precipitation")
    val sRDD = new SRDD[SciTensor](sc, urls, varList, loadNetCDFNDVar, mapOneDayToManyTensorTRMM)
    val Tensor = sRDD.collect()(0)
    assert(true)
  }

  test("SRDDDatasetSerializationTest") {
    val netcdfDataset = NetCDFUtils.loadNetCDFDataSet("src/test/resources/Netcdf/nc_3B42_daily.2008.01.02.7.bin.nc")
    val Dataset = new SciDataset(netcdfDataset)
    val sRDD = sc.parallelize(0 to 5).map(p => Dataset)
    val sRDDDataset = sRDD.collect()(0)
    assert(Dataset == sRDDDataset)
  }

}
