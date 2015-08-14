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

import java.text.SimpleDateFormat

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}
import org.dia.Constants._
import org.dia.TRMMUtils.Parsers
import org.dia.loaders.MergUtils
import org.dia.loaders.NetCDFLoader._
import org.dia.loaders.RandomMatrixLoader._
import org.dia.partitioners.sPartitioner._

import scala.collection.mutable
import scala.io.Source

/**
 * SciSpark contexts extends the existing SparkContext function.
 * However there are many private functions within SparkContext
 * that are useful for catching unwanted calls. Such as
 * executing one of the functions after the SparkContext has been stopped.
 *
 * TODO :: Should we extend SparkContext or modify a copy of SparkContext
 */
class SciSparkContext(val conf: SparkConf) {

  val sparkContext = new SparkContext(conf)
  var extractDate: (String) => (String) = null
  sparkContext.setCheckpointDir("/tmp/scispark_dump")
  sparkContext.setLocalProperty(ARRAY_LIB, ND4J_LIB)

  LogManager.getLogger(Class.forName("com.joestelmach.natty.Parser")).setLevel(org.apache.log4j.Level.OFF)

  def this(url: String, name: String) {
    this(new SparkConf().setMaster(url).setAppName(name))
  }

  def this(url: String, name: String, parser: (String) => (String)) {
    this(new SparkConf().setMaster(url).setAppName(name))
    extractDate = parser
  }
  def setLocalProperty(key: String, value: String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def getConf: SparkConf = sparkContext.getConf

  /**
   * Constructs an sRDD from a file of openDap URL's pointing to NetCDF datasets.
   *
   * TODO :: Support for reading more than one variable
   * TODO :: Properly integrate minimum partitioning
   *
   * @param path Path to a file containing a list of OpenDap URLs
   * @param varName the variable name to search for, by default None is specified in which case all variables are loaded
   * @param minPartitions the minimum number of partitions
   * @return
   */
  @transient def NetcdfFile(path: String,
                                varName: List[String] = Nil,
                            minPartitions: Int = 2
                                 ): (sRDD[sciTensor], mutable.HashMap[Int, String]) = {

    val indexedDateTable = new mutable.HashMap[Int, String]()
    val DateIndexTable = new mutable.HashMap[String, Int]()
    val URLs = Source.fromFile(path).mkString.split("\n").toList

    val orderedDateList = URLs.map(p => {
      //      val source = p.split("/").last.replaceAllLiterally(".", "/")
      //      val date = Parsers.ParseDateFromString(source)
      //      new SimpleDateFormat("YYYY-MM-DD").format(date)
      val source = p.split("_")
      val date = Parsers.ParseDateFromString(source(1))
      new SimpleDateFormat("YYYY-MM-DD").format(date)
    }).sorted

    for(i <- orderedDateList.indices) {
      indexedDateTable += ((i, orderedDateList(i)))
      DateIndexTable += ((orderedDateList(i), i))
    }

    val PartitionSize = (URLs.size.toDouble + minPartitions) / minPartitions.toDouble
    var variables: List[String] = varName

    if (varName == Nil) {
      variables = loadNetCDFVariables(varName.head)
    }

    val rdd = new sRDD[sciTensor](sparkContext, URLs, variables, MergUtils.ReadMergtoPair(Array(9896, 3298))(75.0), mapNUrToOneTensor(PartitionSize.toInt))
    val labeled = rdd.map(p => {
      val source = p.metaData("SOURCE").split("/").last.replaceAllLiterally(".", "/")
      val date = new SimpleDateFormat("YYYY-MM-DD").format(Parsers.ParseDateFromString(source))
      val FrameID = DateIndexTable(date)
      p.insertDictionary(("FRAME", FrameID.toString))
      p
    })

    (labeled, indexedDateTable)
  }

  @transient def randomMatrices(path: String,
                            varName: List[String] = Nil,
                                minPartitions: Int = 2,
                                matrixSize: (Int, Int) = (20, 20)
                             ): (sRDD[sciTensor], mutable.HashMap[Int, String]) = {

    val indexedDateTable = new mutable.HashMap[Int, String]()
    val DateIndexTable = new mutable.HashMap[String, Int]()
    val URLs = Source.fromFile(path).mkString.split("\n").toList

    val orderedDateList = URLs.map(p => {
      val source = p.split("/").last.replaceAllLiterally(".", "/")
      val date = Parsers.ParseDateFromString(source)
      new SimpleDateFormat("YYYY-MM-DD").format(date)
    }).sorted

    for(i <- orderedDateList.indices) {
      indexedDateTable += ((i, orderedDateList(i)))
      DateIndexTable += ((orderedDateList(i), i))
    }

    val PartitionSize = if (URLs.size > minPartitions) (URLs.size.toDouble + minPartitions) / minPartitions.toDouble else 1
    var variables: List[String] = varName

    if (varName == Nil) {
      variables = loadNetCDFVariables(varName.head)
    }

    val rdd = new sRDD[sciTensor](sparkContext, URLs, variables, loadRandomArray(matrixSize), mapNUrToOneTensor(PartitionSize.toInt))
    val labeled = rdd.map(p => {
      val source = p.metaData("SOURCE").split("/").last.replaceAllLiterally(".", "/")
      val date = new SimpleDateFormat("YYYY-MM-DD").format(Parsers.ParseDateFromString(source))
      val FrameID = DateIndexTable(date)
      p.insertDictionary(("FRAME", FrameID.toString))
      p
    })

    (labeled, indexedDateTable)
  }

  @transient def OpenPath(path: String, varName: List[String] = Nil): sRDD[sciTensor] = {
    val datasetPaths = List(path)
    new sRDD[sciTensor](sparkContext, datasetPaths, varName, loadNetCDFNDVars, mapSubFoldersToFolders)
  }


}
