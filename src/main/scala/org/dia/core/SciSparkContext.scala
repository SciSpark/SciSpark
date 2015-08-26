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

import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.dia.Constants._
import org.dia.loaders.MergReader._
import org.dia.loaders.NetCDFReader._
import org.dia.loaders.RandomMatrixReader._
import org.dia.partitioners.sPartitioner._
import org.dia.tensors.BreezeTensor

import scala.io.Source
/**
 * A SciSparkContext is a wrapper for the SparkContext.
 * SciSparkContexts provides existing SparkContext function.
 * However there are many private functions within SparkContext
 * that are useful for catching unwanted calls. Such as
 * executing one of the functions after the SparkContext has been stopped.
 */
class SciSparkContext(val conf: SparkConf) {

  /**
   * Log4j Setup
   * By default the natty Parser log4j messages are turned OFF.
   */
  val DateParserClass = Class.forName("com.joestelmach.natty.Parser")
  val ParserLevel = org.apache.log4j.Level.OFF
  LogManager.getLogger(DateParserClass).setLevel(ParserLevel)

  /**
   * SparkContext setup
   * The default matrix library is Scala Breeze
   */
  val sparkContext = new SparkContext(conf)
  sparkContext.setLocalProperty(ARRAY_LIB, BREEZE_LIB)

  def this(url: String, name: String) {
    this(new SparkConf().setMaster(url).setAppName(name))
  }

  def this(url: String, name: String, parser: (String) => (String)) {
    this(new SparkConf().setMaster(url).setAppName(name))
  }

  def setLocalProperty(key: String, value: String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def getConf: SparkConf = sparkContext.getConf

  /**
   * Constructs an sRDD from a file of URI's pointing to NetCDF datasets and a list of variable names.
   * If no names are provided then all variable arrays in that file are loaded.
   * The URI could be an OpenDapURL or a filesystem path.
   *
   * TODO :: Add support for reading HDFS URIs
   */
  def NetcdfFile(path: String,
                 varName: List[String] = Nil,
                 minPartitions: Int = 2): sRDD[sciTensor] = {

    val URLs = Source.fromFile(path).mkString.split("\n").toList
    val PartitionSize = if (URLs.size > minPartitions) (URLs.size + minPartitions) / minPartitions else 1
    val variables: List[String] = varName

    new sRDD[sciTensor](sparkContext, URLs, variables, loadNetCDFNDVars, MapNUrl(PartitionSize))
  }

  /**
   * Constructs a random sRDD from a file of URI's, a list of variable names, and matrix dimensions.
   * The seed for matrix values is the path values, so the same input set will yield the the same data.
   *
   */
  def randomMatrices(path: String,
                     varName: List[String] = Nil,
                     matrixSize: (Int, Int),
                     minPartitions: Int = 2): sRDD[sciTensor] = {

    val URLs = Source.fromFile(path).mkString.split("\n").toList
    val PartitionSize = if (URLs.size > minPartitions) (URLs.size + minPartitions) / minPartitions else 1
    val variables: List[String] = varName

    new sRDD[sciTensor](sparkContext, URLs, variables, loadRandomArray(matrixSize), MapNUrl(PartitionSize))
  }

  /**
   * Constructs an sRDD given a file of URI's pointing to MERG files, a list of variables names,
   * and matrix dimensions. By default the variable name is set to TMP, the dimensions are 9896 x 3298
   * and the value offset is 75.
   */
  def mergeFile(path: String,
                varName: List[String] = List("TMP"),
                shape: Array[Int] = Array(9896, 3298),
                offset: Double = 75,
                minPartitions: Int = 2): sRDD[sciTensor] = {
    
    val URLs = Source.fromFile(path).mkString.split("\n").toList
    val PartitionSize = if (URLs.size > minPartitions) (URLs.size + minPartitions) / minPartitions else 1

    new sRDD[sciTensor](sparkContext, URLs, varName, LoadMERGArray(shape, offset), MapNUrl(PartitionSize))
  }

  /**
   * Constructs an RDD given a file of URI's pointing to MERG files, a list of variable names,
   * and matrix dimensions. By default the variable name is set to TMP, the dimensions are 9896 x 3298
   * and the value offset is 75.
   *
   * Note that since the files are read from HDFS, the binaryFiles function is used which is called
   * from SparkContext. This is why a normal RDD is returned instead of an sRDD.
   *
   * TODO :: Create an sRDD instead of a normal RDD
   */
  def mergDFSFile(path: String,
                      varName: List[String] = List("TMP"),
                  offset: Double = 75,
                      shape: Array[Int] = Array(9896, 3298),
                  minPartitions: Int = 2): RDD[sciTensor] = {

    val textFiles = sparkContext.binaryFiles(path, minPartitions)
    val rdd = textFiles.map(p => {
      val byteArray = p._2.toArray()
      val doubleArray = ReadMergBytetoJavaArray(byteArray, offset, shape)
      val absT = new BreezeTensor(doubleArray, shape)
      val sciT = new sciTensor("TMP", absT)
      sciT.insertDictionary(("SOURCE", p._1))
      sciT
    })
    rdd
  }

  /**
   * Constructs an sRDD given a nested directories of NetCDF files.
   */
  def OpenPath(path: String, varName: List[String] = Nil): sRDD[sciTensor] = {
    val datasetPaths = List(path)
    new sRDD[sciTensor](sparkContext, datasetPaths, varName, loadNetCDFNDVars, mapSubFoldersToFolders)
  }

}
