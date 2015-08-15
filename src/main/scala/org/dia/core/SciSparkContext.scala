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
import org.dia.loaders.MergUtils
import org.dia.loaders.NetCDFLoader._
import org.dia.loaders.RandomMatrixLoader._
import org.dia.partitioners.sPartitioner._
import org.dia.tensors.BreezeTensor

import scala.io.Source
/**
 * SciSpark contexts extends the existing SparkContext function.
 * However there are many private functions within SparkContext
 * that are useful for catching unwanted calls. Such as
 * executing one of the functions after the SparkContext has been stopped.
 */
class SciSparkContext(val conf: SparkConf) {

  val sparkContext = new SparkContext(conf)
  sparkContext.setLocalProperty(ARRAY_LIB, BREEZE_LIB)

  LogManager.getLogger(Class.forName("com.joestelmach.natty.Parser")).setLevel(org.apache.log4j.Level.OFF)

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
   * Constructs an sRDD from a file of openDap URL's pointing to NetCDF datasets.
   *
   * @param path Path to a file containing a list of OpenDap URLs
   * @param varName the variable name to search for, by default None is specified in which case all variables are loaded
   * @param minPartitions the minimum number of partitions
   * @return
   */
  def NetcdfFile(path: String,
                 varName: List[String] = Nil,
                 minPartitions: Int = 2): sRDD[sciTensor] = {

    val URLs = Source.fromFile(path).mkString.split("\n").toList
    val PartitionSize = if (URLs.size > minPartitions) (URLs.size.toDouble + minPartitions) / minPartitions.toDouble else 1
    val variables: List[String] = if (varName == Nil) loadNetCDFVariables(varName.head) else varName

    val rdd = new sRDD[sciTensor](sparkContext, URLs, variables, loadNetCDFNDVars, mapNUrToOneTensor(PartitionSize.toInt))
    rdd
  }

  def randomMatrices(path: String,
                     varName: List[String] = Nil,
                     minPartitions: Int = 2,
                     matrixSize: (Int, Int) = (20, 20)): sRDD[sciTensor] = {

    val URLs = Source.fromFile(path).mkString.split("\n").toList
    val PartitionSize = if (URLs.size > minPartitions) (URLs.size + minPartitions) / minPartitions else 1
    val variables: List[String] = if (varName == Nil) loadNetCDFVariables(varName.head) else varName

    val rdd = new sRDD[sciTensor](sparkContext, URLs, variables, loadRandomArray(matrixSize), mapNUrToOneTensor(PartitionSize))
    rdd
  }

  def mergeFile(path: String,
                varName: List[String] = List("TMP"),
                minPartitions: Int = 2,
                shape : Array[Int] = Array(9896, 3298),
                 offset : Double = 75): sRDD[sciTensor] = {
    val URLs = Source.fromFile(path).mkString.split("\n").toList
    val PartitionSize = if (URLs.size > minPartitions) (URLs.size + minPartitions) / minPartitions else 1
    val rdd = new sRDD[sciTensor](sparkContext, URLs, varName, MergUtils.ReadMergtoNDArray(shape, offset), mapNUrToOneTensor(PartitionSize))
    rdd
  }

  def mergTachyonFile(path: String,
                      varName: List[String] = List("TMP"),
                      minPartitions: Int = 2,
                      shape: Array[Int] = Array(9896, 3298),
                      offset: Double = 75): RDD[sciTensor] = {
    val textFiles = sparkContext.binaryFiles(path, minPartitions)
    val rdd = textFiles.map(p => {
      val byteArray = p._2.toArray
      val doubleArray = MergUtils.ReadMergByteArray(byteArray, offset, shape)
      val absT = new BreezeTensor(doubleArray, shape)
      val sciT = new sciTensor("TMP", absT)
      sciT.insertDictionary(("SOURCE", p._1))
      sciT
    })
    rdd
  }

  def OpenPath(path: String, varName: List[String] = Nil): sRDD[sciTensor] = {
    val datasetPaths = List(path)
    new sRDD[sciTensor](sparkContext, datasetPaths, varName, loadNetCDFNDVars, mapSubFoldersToFolders)
  }

}
