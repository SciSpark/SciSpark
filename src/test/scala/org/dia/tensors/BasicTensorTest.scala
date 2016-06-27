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
package org.dia.tensors

import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite
import org.nd4s.Implicits._
import org.slf4j.Logger
import org.dia.loaders.TestMatrixReader._
import org.dia.partitioners.SPartitioner._
import org.dia.testenv.SparkTestConstants
import org.dia.core.{ SRDD, SciTensor }

/**
 * Tests basic tensor functionality.
 */
class BasicTensorTest extends FunSuite {

  val fakeURI = List("0000010100")
  val varName = List("randVar", "randVar_1")
  val st = SparkTestConstants.sc.sparkContext
  val sRDD = new SRDD[SciTensor](st, fakeURI, varName, loadTestArray, mapOneUrl)
  val fakeURIs = List("0000010100","0000010101", "0000010102", "0000010103", "0000010104" ,"0000010105")
  val uSRDD = new SRDD[SciTensor](st, fakeURIs, varName, loadTestUniformArray, mapOneUrl)
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
  * Test relational operators
  **/

  test("filter") {
    logger.info("In filter test ...")
    val dense = Nd4j.create(Array[Double](1, 241, 241, 1), Array(2, 2))
    val t = dense.map(p => if (p < 241.0) p else 0.0)
    logger.info(t.toString)
    assert(true)
  }

  test("setMask") {
    logger.info("In setMask test ...")
    val dense = Nd4j.create(Array[Double](1, 241, 241, 1), Array(2, 2))
    val t = dense.map(p => if (p < 241.0) p else 100)
    val Nd4jt1 = new Nd4jTensor(t)
    val Nd4jt2 = new Nd4jTensor(dense)
    val filteredNd4jt2 = Nd4jt2.setMask(100) < 241.0
    assert(Nd4jt1 == filteredNd4jt2)
  }

 test("filterLessThan") {
    logger.info("In filterLessThan test ...")
    val t = sRDD.map(p => p("randVar") < 241.0)
    logger.info("The sciTensor is: "+ t.collect().toList)
    logger.info("The values are: "+ t.collect().toList(0).variables.values.toList)
    val tVals = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    val count = tVals.filter(_.toDouble < 241.0).length - tVals.filter(_.toDouble == 0.0).length
    logger.info(count + " are less than 241.0")  

    if (count == 12){
      assert(true)
      }else{
        assert(false)
      }   
  }

  test("filterLessThanEquals") {
    logger.info("In filterLessThanEquals test ...")
    val t = sRDD.map(p => p("randVar") <= 241.0)
    logger.info("The sciTensor is: "+ t.collect().toList)
    logger.info("The values are: "+ t.collect().toList(0).variables.values.toList)
    val tVals = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    val count = tVals.filter(_.toDouble != 0.0).length //- tVals.filter(_.toDouble == 0.0).length
    logger.info(count + " are less than or equals to 241.0")  
     
    if (count == 22){
      assert(true)
      }else{
        assert(false)
      }   
  }

  test("filterGreaterThan") {
    logger.info("In filterGreaterThan test ...")
    val t = sRDD.map(p => p("randVar") > 241.0)
    logger.info("The sciTensor is: "+ t.collect().toList)
    logger.info("The values are: "+ t.collect().toList(0).variables.values.toList)
    val tVals = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    val count = tVals.filter(_.toDouble != 0.0).length 
    logger.info(count + " are greater than 241.0")  
     
    if (count == 8){
      assert(true)
      }else{
        assert(false)
      }   
  }

  test("filterGreaterThanEquals") {
    logger.info("In filterGreaterThanEquals test ...")
    val t = sRDD.map(p => p("randVar") >= 241.0)
    logger.info("The sciTensor is: "+ t.collect().toList)
    logger.info("The values are: "+ t.collect().toList(0).variables.values.toList)
    val tVals = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    val count = tVals.filter(_.toDouble != 0.0).length 
    logger.info(count + " are greater than or equals to 241.0")  
     
    if (count == 18){
      assert(true)
      }else{
        assert(false)
      }   
  }

  test("filterEquals") {
    logger.info("In filterLessThan test ...")
    val t = sRDD.map(p => p("randVar") := 241.0)
    logger.info("The sciTensor is: "+ t.collect().toList)
    logger.info("The values are: "+ t.collect().toList(0).variables.values.toList)
    val tVals = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    val count = tVals.filter(_.toDouble != 0.0).length 
    logger.info(count + " are equal to 241.0")  
     
    if (count == 10){
      assert(true)
      }else{
        assert(false)
      }   
  }

  test("filterNotEquals") {
    logger.info("In filterNotEquals test ...")
    val t = sRDD.map(p => p("randVar") != 241.0)
    logger.info("The sciTensor is: "+ t.collect().toList)
    logger.info("The values are: "+ t.collect().toList(0).variables.values.toList)
    val tVals = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    val count = tVals.filter(_.toDouble  != 0.0).length 
    logger.info(count + " are not equals to 241.0")  
     
    if (count == 20){
      assert(true)
      }else{
        assert(false)
      }   
  }

  /**
  * End Test relational operators
  **/

  /**
  * Test slicing
  **/
  test("Nd4sSlice") {
    logger.info("In Nd4sSlice test ...")
    val nd = Nd4j.create((0d to 8d by 1d).toArray, Array(4, 2))
    logger.info(nd.toString)
    logger.info("slicing")
    logger.info(nd(0 -> 1, ->).toString)
    assert(true)
  }

  /**
  * Test matrix artihmetic
  **/
  test("matrixAddition"){
    logger.info("In inPlaceMatrixAddition test ...")
    logger.info("The sciTensor is: "+ uSRDD.collect().toList)
    logger.info("The values are: "+ uSRDD.collect().toList(0).variables.values.toList)
    // test scalar addition
    val s = uSRDD.map(p => p("randVar") + 2.0)
    logger.info("Addition: " + s.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!=""))
    val y = s.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    if (y.filter(_.toDouble  != 3.0).length == 0){
        assert(true)
    } else{
      assert(false)
    }
    // test scalar addition
    val t = uSRDD.map(p => p("randVar") :+ 2.0)
    logger.info("Addition: " + t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!=""))
    val y1 = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    if (y1.filter(_.toDouble  != 3.0).length == 0){
        assert(true)
    } else{
      assert(false)
    }
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
    for (eachArray <- ewList){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=2.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    } 
    val ew1 = ordered.map(p => p._1("randVar") :+ p._2("randVar"))
    val ewList1 = ew1.collect().toList
    logger.info("List length before is: " + c.length + " and after is length " + ewList1.length)
    for (eachArray <- ewList1){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=2.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    }   
  }

  test("matrixSubtraction"){
    logger.info("In inPlaceMatrixSubtraction test ...")
    logger.info("The sciTensor is: "+ uSRDD.collect().toList)
    logger.info("The values are: "+ uSRDD.collect().toList(0).variables.values.toList)
    // test scalar subtraction
    val s = uSRDD.map(p => p("randVar") - 2.0)
    logger.info("Subtraction: " + s.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!=""))
    val y = s.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    if (y.filter(_.toDouble  != -1.0).length == 0){
        assert(true)
    } else{
      assert(false)
    }
    // test scalar subtraction
    val t = uSRDD.map(p => p("randVar") :- 2.0)
    val y1 = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    if (y1.filter(_.toDouble  != -1.0).length == 0){
        assert(true)
    } else{
      assert(false)
    }
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
    for (eachArray <- ewList){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=0.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    } 

    val ew1 = ordered.map(p => p._2("randVar") :- p._1("randVar"))
    val ewList1 = ew1.collect().toList
    logger.info("List length before is: " + c.length + " and after is length " + ewList1.length)
    for (eachArray <- ewList1){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=0.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    }   
  }

  test("matrixDivision"){
    logger.info("In matrixDivision test ...")
    logger.info("The sciTensor is: "+ uSRDD.collect().toList)
    logger.info("The values are: "+ uSRDD.collect().toList(0).variables.values.toList)
    // test scalar division
    val s = uSRDD.map(p => p("randVar") / 2.0)
    logger.info("Division: " + s.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!=""))
    val y = s.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    if (y.filter(_.toDouble  != 0.5).length == 0){
        assert(true)
    } else{
      assert(false)
    }
    val t = uSRDD.map(p => p("randVar") :/ 2.0)
    logger.info("Division: " + t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!=""))
    val y1 = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    if (y1.filter(_.toDouble  != 0.5).length == 0){
        assert(true)
    } else{
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
    for (eachArray <- ewList){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=1.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    }
    
    val ew1 = ordered.map(p => p._1("randVar") / p._2("randVar"))
    val ewList1 = ew1.collect().toList
    logger.info("List length before is: " + c.length + " and after is length " + ewList1.length)
    for (eachArray <- ewList1){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=1.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    }      
  }

  test("matrixMultiplication"){
    logger.info("In matrixMultiplication test ...")
    logger.info("The sciTensor is: "+ uSRDD.collect().toList)
    logger.info("The values are: "+ uSRDD.collect().toList(0).variables.values.toList)
    // test scalar multiplication
    val s = uSRDD.map(p => p("randVar") * 2.0)
    logger.info("Multiplication: " + s.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!=""))
    val y = s.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    if (y.filter(_.toDouble  != 2.0).length == 0){
        assert(true)
    } else{
      assert(false)
    }
    // test scalar multiplication
    val t = uSRDD.map(p => p("randVar") :* 2.0)
    val y1 = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    if (y1.filter(_.toDouble  != 2.0).length == 0){
        assert(true)
    } else{
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
    for (eachArray <- ewList){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=1.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    }    
  
    val ew1 = ordered.map(p => p._1("randVar") :* p._2("randVar"))
    val ewList1 = ew.collect().toList
    logger.info("List length before is: " + c.length + " and after is length " + ewList1.length)
    for (eachArray <- ewList1){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=1.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    }    
  }  

  /**
  * Test data retrieval - values,shape
  **/
  test("getVarData"){
    println("getVarData test ...")
    val x = Array(240.0,241.0,240.0,241.0,241.0,230.0,231.0,240.0,222.0,241.0,242.0,243.0,244.0,241.0,232.0,240.0,
      241.0,230.0,231.0,241.0,240.0,241.0,240.0,242.0,241.0,242.0,243.0,244.0,241.0,242.0)
    val t = sRDD.map(p => (p("randVar").data, p("randVar").shape))
    val varData = t.collect()(0)
    logger.info("The data is: "+ varData._1.mkString(" ") + "\nShape: (" + varData._2.mkString(" , ")+")")
    if (varData._2(0) == 6 && varData._2(1) == 5 && varData._1.length == 30 && varData._1.sameElements(x)){
      assert(true)
    }else{
      assert(false)
    }
  }

  /**
  * Test varInUse
  **/
  test("varInUse"){
    println("varInUse test ...")
    val t = sRDD.map(p => (p("randVar").data, p("randVar").shape))
    val varData = t.collect()(0)
    logger.info("The randVar data is: "+ varData._1.mkString(" ") + "\nShape: (" + varData._2.mkString(" , ")+")")

    val t1 = sRDD.map(p => (p("randVar_1").data, p("randVar_1").shape))
    val varData1 = t1.collect()(0)
    logger.info("The randVar_1 data is: "+ varData1._1.mkString(" ") + "\nShape: (" + varData1._2.mkString(" , ")+")")

    val tdefault = sRDD.map(p => (p.data, p.shape))
    val varDataDef = tdefault.collect()(0)
    logger.info("The varInUse default data is: "+ varDataDef._1.mkString(" ") + "\nShape: (" + varDataDef._2.mkString(" , ")+")")
    
    if (!(varData._1.sameElements(varData1._1)) && (varData1._1.sameElements(varDataDef._1))){
      assert(true)
    }else{
      assert(false)
    }    
  }
}