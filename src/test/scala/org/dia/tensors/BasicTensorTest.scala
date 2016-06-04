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

import org.dia.core.{ SRDD, SciTensor }
import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite
import org.nd4s.Implicits._
import org.dia.loaders.TestMatrixReader._
import org.dia.partitioners.SPartitioner._
import org.dia.testenv.SparkTestConstants

/**
 * Tests basic tensor functionality.
 */
class BasicTensorTest extends FunSuite {

  val fakeURI = List("0000010100")
  val varName = List("randVar")
  val st = SparkTestConstants.sc.sparkContext
  val sRDD = new SRDD[SciTensor](st, fakeURI, varName, loadTestArray, mapOneUrl)
  val fakeURIs = List("0000010100","0000010101", "0000010102", "0000010103", "0000010104" ,"0000010105")
  val uSRDD = new SRDD[SciTensor](st, fakeURIs, varName, loadTestUniformArray, mapOneUrl)
  
  /**
  * Test relational operators
  **/

  test("filter") {
    println("In filter test ...")
    val dense = Nd4j.create(Array[Double](1, 241, 241, 1), Array(2, 2))
    val t = dense.map(p => if (p < 241.0) p else 0.0)
    println(t)
    assert(true)
  }

 test("filterLessThan") {
    println("In filterLessThan test ...")
    val t = sRDD.map(p => p("randVar") < 241.0)
    println("The sciTensor is: "+ t.collect().toList)
    println("The values are: "+ t.collect().toList(0).variables.values.toList)
    val tVals = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    val count = tVals.filter(_.toDouble < 241.0).length - tVals.filter(_.toDouble == 0.0).length
    println(count + " are less than 241.0")  

    if (count == 12){
      assert(true)
      }else{
        assert(false)
      }   
  }

  test("filterLessThanEquals") {
    println("In filterLessThanEquals test ...")
    val t = sRDD.map(p => p("randVar") <= 241.0)
    println("The sciTensor is: "+ t.collect().toList)
    println("The values are: "+ t.collect().toList(0).variables.values.toList)
    val tVals = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    val count = tVals.filter(_.toDouble != 0.0).length //- tVals.filter(_.toDouble == 0.0).length
    println(count + " are less than or equals to 241.0")  
     
    if (count == 22){
      assert(true)
      }else{
        assert(false)
      }   
  }

  test("filterGreaterThan") {
    println("In filterGreaterThan test ...")
    val t = sRDD.map(p => p("randVar") > 241.0)
    println("The sciTensor is: "+ t.collect().toList)
    println("The values are: "+ t.collect().toList(0).variables.values.toList)
    val tVals = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    val count = tVals.filter(_.toDouble != 0.0).length 
    println(count + " are greater than 241.0")  
     
    if (count == 8){
      assert(true)
      }else{
        assert(false)
      }   
  }

  test("filterGreaterThanEquals") {
    println("In filterGreaterThanEquals test ...")
    val t = sRDD.map(p => p("randVar") >= 241.0)
    println("The sciTensor is: "+ t.collect().toList)
    println("The values are: "+ t.collect().toList(0).variables.values.toList)
    val tVals = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    val count = tVals.filter(_.toDouble != 0.0).length 
    println(count + " are greater than or equals to 241.0")  
     
    if (count == 18){
      assert(true)
      }else{
        assert(false)
      }   
  }

  test("filterEquals") {
    println("In filterLessThan test ...")
    val t = sRDD.map(p => p("randVar") := 241.0)
    println("The sciTensor is: "+ t.collect().toList)
    println("The values are: "+ t.collect().toList(0).variables.values.toList)
    val tVals = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    val count = tVals.filter(_.toDouble != 0.0).length 
    println(count + " are equal to 241.0")  
     
    if (count == 10){
      assert(true)
      }else{
        assert(false)
      }   
  }

  test("filterNotEquals") {
    println("In filterNotEquals test ...")
    val t = sRDD.map(p => p("randVar") != 241.0)
    println("The sciTensor is: "+ t.collect().toList)
    println("The values are: "+ t.collect().toList(0).variables.values.toList)
    val tVals = t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    val count = tVals.filter(_.toDouble  != 0.0).length 
    println(count + " are not equals to 241.0")  
     
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
    println("In Nd4sSlice test ...")
    val nd = Nd4j.create((0d to 8d by 1d).toArray, Array(4, 2))
    println(nd)
    println("slicing")
    println(nd(0 -> 1, ->))
    assert(true)
  }

  /**
  * Test matrix artihmetic
  **/
  test("matrixAddition"){
    println("In inPlaceMatrixAddition test ...")
    println("The sciTensor is: "+ uSRDD.collect().toList)
    println("The values are: "+ uSRDD.collect().toList(0).variables.values.toList)
    // test scalar addition
    val s = uSRDD.map(p => p("randVar") + 2.0)
    println("Addition: " + s.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!=""))
    val y = s.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    if (y.filter(_.toDouble  != 3.0).length == 0){
        assert(true)
    } else{
      assert(false)
    }
    // test scalar addition
    val t = uSRDD.map(p => p("randVar") :+ 2.0)
    println("Addition: " + t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!=""))
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
    for (i <- c) println(i._1.metaData + " connected to " + i._2.metaData)

    val ew = ordered.map(p => p._1("randVar") + p._2("randVar"))
    val ewList = ew.collect().toList
    println("List length before is: " + c.length + " and after is length " + ewList.length)
    for (eachArray <- ewList){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=2.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    } 
    val ew1 = ordered.map(p => p._1("randVar") :+ p._2("randVar"))
    val ewList1 = ew1.collect().toList
    println("List length before is: " + c.length + " and after is length " + ewList1.length)
    for (eachArray <- ewList1){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=2.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    }   
  }

  test("matrixSubtraction"){
    println("In inPlaceMatrixSubtraction test ...")
    println("The sciTensor is: "+ uSRDD.collect().toList)
    println("The values are: "+ uSRDD.collect().toList(0).variables.values.toList)
    // test scalar subtraction
    val s = uSRDD.map(p => p("randVar") - 2.0)
    println("Subtraction: " + s.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!=""))
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
    for (i <- c) println(i._1.metaData + " connected to " + i._2.metaData)

    val ew = ordered.map(p => p._2("randVar") - p._1("randVar"))
    val ewList = ew.collect().toList
    println("List length before is: " + c.length + " and after is length " + ewList.length)
    for (eachArray <- ewList){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=0.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    } 

    val ew1 = ordered.map(p => p._2("randVar") :- p._1("randVar"))
    val ewList1 = ew1.collect().toList
    println("List length before is: " + c.length + " and after is length " + ewList1.length)
    for (eachArray <- ewList1){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=0.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    }   
  }

  test("matrixDivision"){
    println("In matrixDivision test ...")
    println("The sciTensor is: "+ uSRDD.collect().toList)
    println("The values are: "+ uSRDD.collect().toList(0).variables.values.toList)
    // test scalar division
    val s = uSRDD.map(p => p("randVar") / 2.0)
    println("Division: " + s.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!=""))
    val y = s.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!="")
    if (y.filter(_.toDouble  != 0.5).length == 0){
        assert(true)
    } else{
      assert(false)
    }
    val t = uSRDD.map(p => p("randVar") :/ 2.0)
    println("Division: " + t.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!=""))
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
    for (i <- c) println(i._1.metaData + " connected to " + i._2.metaData)

    val ew = ordered.map(p => p._1("randVar") :/ p._2("randVar"))
    val ewList = ew.collect().toList
    println("List length before is: " + c.length + " and after is length " + ewList.length)
    for (eachArray <- ewList){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=1.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    }
    
    val ew1 = ordered.map(p => p._1("randVar") / p._2("randVar"))
    val ewList1 = ew1.collect().toList
    println("List length before is: " + c.length + " and after is length " + ewList1.length)
    for (eachArray <- ewList1){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=1.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    }      
  }

  test("matrixMultiplication"){
    println("In matrixMultiplication test ...")
    println("The sciTensor is: "+ uSRDD.collect().toList)
    println("The values are: "+ uSRDD.collect().toList(0).variables.values.toList)
    // test scalar multiplication
    val s = uSRDD.map(p => p("randVar") * 2.0)
    println("Multiplication: " + s.collect().toList(0).variables.values.toList(0).toString.split("  ").toList.filter(_!=""))
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
    for (i <- c) println(i._1.metaData + " connected to " + i._2.metaData)
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
    println("List length before is: " + c.length + " and after is length " + ewList1.length)
    for (eachArray <- ewList1){
      if(eachArray.variables.values.toList(0).toString.split("  ").toList.filter(_!="").filter(_!=1.0).length == 30){
        assert(true)
      }else{
        assert(false)
      }
    }    
  }  

}