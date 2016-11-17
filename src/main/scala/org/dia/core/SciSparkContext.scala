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

import java.net.URI

import scala.collection.mutable
import scala.io.Source

import org.apache.hadoop.conf._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.log4j.LogManager

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame}

import org.dia.Constants._
import org.dia.loaders.MergReader._
import org.dia.loaders.NetCDFReader._
import org.dia.loaders.RandomMatrixReader._
import org.dia.partitioners.SPartitioner._
import org.dia.tensors.{AbstractTensor, BreezeTensor, Nd4jTensor}
import org.dia.utils.NetCDFUtils
import org.dia.utils.WWLLNUtils

/**
 * A SciSparkContext is a wrapper for the SparkContext.
 * SciSparkContext provides existing SparkContext functionality.
 * However there are many private functions within SparkContext
 * that are useful for catching unwanted calls. Such as
 * executing one of the functions after the SparkContext has been stopped.
 */

// Define WWLLN schema for DataFrame
case class WWLLN(lDateTime: String, lat: Double, lon: Double, resFit: Double, numStations: Integer)

class SciSparkContext (@transient val sparkContext: SparkContext) extends Serializable {
  /**
   * Log4j Setup
   * By default the natty Parser log4j messages are turned OFF.
   */
  // scalastyle:off classforname
  val DateParserClass = Class.forName("com.joestelmach.natty.Parser")
  // scalastyle:on classforname
  val ParserLevel = org.apache.log4j.Level.OFF
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  val defaultPartitions = sparkContext.defaultMinPartitions
  var HTTPCredentials: mutable.Seq[(String, String, String)] = mutable.Seq()
  LogManager.getLogger(DateParserClass).setLevel(ParserLevel)

  /**
   * SparkContext setup
   * The default matrix library is Scala Breeze
   */
  sparkContext.setLocalProperty(ARRAY_LIB, BREEZE_LIB)


  /**
   * We use kryo serialization.
   * As of nd4j 0.5.0 we need to add the Nd4jRegistrar to avoid nd4j serialization errors.
   * These errors come in the form of NullPointerErrors.
   * @param conf Configuration for a spark application
   */
  def this(conf: SparkConf) {
    this(new SparkContext(conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrator", "org.nd4j.Nd4jRegistrator")
      .set("spark.kryoserializer.buffer.max", "256MB")))
  }

  def this(uri: String, name: String) {
    this(new SparkConf().setMaster(uri).setAppName(name))
  }

  def this(uri: String, name: String, parser: (String) => (String)) {
    this(new SparkConf().setMaster(uri).setAppName(parser(name)))
  }

  def setLocalProperty(key: String, value: String): Unit = {
    sparkContext.setLocalProperty(key, value)
  }

  def getConf: SparkConf = sparkContext.getConf

  def stop: Unit = sparkContext.stop()

  /**
   * Adds http credentials which is then registered by any function
   * using ucar's httpservices. Namely, if you are reading any opendap file
   * that requires you to enter authentication credentials in the browser.
   * Some datasets (like those hosted by gesdisc) require http credentials
   * for authentiation and access.
   *
   * @param uri webpage url
   * @param username the username used for authentication
   * @param password the password used for authentication
   */
  def addHTTPCredential(uri: String, username: String, password: String): Unit = {
    HTTPCredentials = HTTPCredentials.+:((uri, username, password))
  }

  /**
   * Given an URI string the relevant dataset is loaded into an RDD.
   * If the URI string ends in a .txt and consists of line separated URI's pointing
   * to netCDF files then NetcdfDataset is called. Otherwise NetcdfRandomAccessDatasets
   * is called.
   *
   * @param path The URI string pointing to
   *             A) a directory of netCDF files
   *             B) a .txt file consisting of line separated OpenDAP URLs
   * @param vars The variables to be extracted from the dataset
   * @param partitions The number of partitions the data should be split into
   */
  def sciDatasets(path : String, vars : List[String] = Nil, partitions : Int = defaultPartitions): RDD[SciDataset] = {
    val uri = new URI(path)
    if (uri.getPath.endsWith(".txt")) {
      netcdfDatasetList(path, vars, partitions)
    } else {
      netcdfRandomAccessDatasets(path, vars, partitions)
    }
  }

  /**
   * Constructs an RDD of SciDatasets from a file of URI's pointing to a NetCDF dataset and a list of
   * variable names. If no names are provided then all variable arrays in the file are loaded.
   * The URI could be an OpenDapURL or a filesystem path.
   *
   * For reading from HDFS check NetcdfRandomAccessDatasets
   */
  def netcdfDatasetList(
      path: String,
      vars: List[String] = Nil,
      partitions: Int = defaultPartitions): RDD[SciDataset] = {

    val creds = HTTPCredentials.clone()
    val URIsFile = sparkContext.textFile(path, partitions)
    val rdd = URIsFile.map(p => {
      for ((uri, username, password) <- creds) NetCDFUtils.setHTTPAuthentication(uri, username, password)
      val netcdfDataset = NetCDFUtils.loadNetCDFDataSet(p)
      if(vars.nonEmpty) {
        new SciDataset(netcdfDataset, vars)
      } else {
        new SciDataset(netcdfDataset)
      }
    })
    rdd
  }

  /**
   * Constructs an SRDD from a file of URI's pointing to NetCDF datasets and a list of variable names.
   * If no names are provided then all variable arrays in the file are loaded.
   * The URI could be an OpenDapURL or a filesystem path.
   *
   * For reading from HDFS check NetcdfDFSFile.
   */
  def netcdfFileList(
      path: String,
      varName: List[String] = Nil,
      partitions: Int = defaultPartitions): RDD[SciTensor] = {

    val URIsFile = sparkContext.textFile(path, partitions)
    val creds = HTTPCredentials.clone()
    val rdd = URIsFile.map(p => {
      for ((uri, username, password) <- creds) NetCDFUtils.setHTTPAuthentication(uri, username, password)
      val variableHashTable = new mutable.HashMap[String, AbstractTensor]
      // note that split, if it doesn't find the token, will result in just an empty string
      var uriVars = p.split("\\?").drop(1).mkString.split(",").filter(p => p != "")
      val mainURL = p.split("\\?").take(1).mkString + "?"
      var varDapPart = ""

      if (uriVars.length > 0) {
        varName.foreach(y => {
          uriVars.foreach(i => {
            if (i.contains(y)) varDapPart = i
          })
          if (varDapPart != "") {
            val arrayandShape = loadNetCDFNDVar(mainURL + varDapPart, y)
            val absT = new Nd4jTensor(arrayandShape)
            variableHashTable += ((y, absT))
          } else {
            println(y + " is not available from the URL")
          }
        })
      } else {
        varName.foreach(y => {
          val arrayandShape = loadNetCDFNDVar(p, y)
          val absT = new Nd4jTensor(arrayandShape)
          variableHashTable += ((y, absT))
        })
      }
      val sciT = new SciTensor(variableHashTable)
      sciT.insertDictionary(("SOURCE", p))
      sciT
    })
    rdd
  }

  /**
   * Constructs an SRDD from a file of URI's pointing to NetCDF datasets and a list of variable names.
   * If no names are provided then all variable arrays in the file are loaded.
   * The URI could be an OpenDapURL or a filesystem path.
   *
   * For reading from HDFS check NetcdfDFSFile.
   */
  def netcdfRandomAccessDatasets(
      path: String,
      varName: List[String] = Nil,
      partitions: Int = defaultPartitions): RDD[SciDataset] = {

    val fs = FileSystem.get(new URI(path), new Configuration())
    val FileStatuses = fs.listStatus(new Path(path))
    val fileNames = FileStatuses.map(p => p.getPath.getName)
    val nameRDD = sparkContext.parallelize(fileNames, partitions)

    nameRDD.map(fileName => {
      val k = NetCDFUtils.loadDFSNetCDFDataSet(path, path + fileName, 4000)
      varName match {
        case Nil => new SciDataset (k)
        case s : List[String] => new SciDataset(k, s)
      }
    })
  }

  /**
   * Constructs an RDD given a URI pointing to an HDFS directory of Netcdf files and a list of variable names.
   * Note that since the files are read from HDFS, the binaryFiles function is used which is called
   * from SparkContext. This is why a normal RDD is returned instead of an SRDD.
   *
   * TODO :: Create an SRDD instead of a normal RDD
   */
  def netcdfDFSFiles(
      path: String,
      varName: List[String] = Nil,
      partitions: Int = defaultPartitions): RDD[SciTensor] = {

    val textFiles = sparkContext.binaryFiles(path, partitions)
    val rdd = textFiles.map(p => {
      val byteArray = p._2.toArray()
      val dataset = loadNetCDFFile(p._1, byteArray)
      val variableHashTable = new mutable.LinkedHashMap[String, AbstractTensor]

      varName.foreach(y => {
        val arrayandShape = loadNetCDFNDVar(dataset, y)
        val absT = new Nd4jTensor(arrayandShape)
        variableHashTable += ((y, absT))
      })
      val sciT = new SciTensor(variableHashTable)
      sciT.insertDictionary(("SOURCE", p._1))
      sciT
    })
    rdd
  }

  /**
   * Constructs an RDD given a URI pointing to an HDFS directory of Netcdf files and a list of variable names.
   * Note that since the files are read from HDFS, the binaryFiles function is used which is called
   * from SparkContext.
   * netcdfWholeDatasets should only be used for reading large subsets of variables or all the
   * variables in NetCDF Files.
   *
   * @param path the path to read from. The format for HDFS directory is : hdfs://HOSTNAME:<port no.>
   * @param varNames the names of variables to extract. If none are provided, then all variables are reaad.
   * @param partitions The number of partitions to split the dataset into
   * @return
   */
  def netcdfWholeDatasets(
      path: String,
      varNames: List[String] = Nil,
      partitions: Int = defaultPartitions): RDD[SciDataset] = {

    val textFiles = sparkContext.binaryFiles(path, partitions)
    textFiles.map(p => {
      val byteArray = p._2.toArray()
      val dataset = loadNetCDFFile(p._1, byteArray)
      varNames match {
        case Nil => new SciDataset(dataset)
        case s => new SciDataset(dataset, varNames)
      }
    })
  }


  /**
   * Constructs a random SRDD from a file of URI's, a list of variable names, and matrix dimensions.
   * The seed for matrix values is the path values, so the same input set will yield the same data.
   *
   */
  def randomMatrices(
      path: String,
      varName: List[String] = Nil,
      matrixSize: (Int, Int),
      partitions: Int = defaultPartitions): SRDD[SciTensor] = {

    val URIs = Source.fromFile(path).mkString.split("\n").toList
    val partitionSize = if (URIs.size > partitions) (URIs.size + partitions) / partitions else 1
    val variables: List[String] = varName

    new SRDD[SciTensor](sparkContext, URIs, variables, loadRandomArray(matrixSize), mapNUri(partitionSize))
  }

  /**
   * Constructs an SRDD given a file of URI's pointing to MERG files, a list of variables names,
   * and matrix dimensions. By default the variable name is set to TMP, the dimensions are 9896 x 3298
   * and the value offset is 75.
   */
  def mergeFile(
      path: String,
      varName: List[String] = List("TMP"),
      shape: Array[Int] = Array(9896, 3298),
      offset: Double = 75,
      partitions: Int = defaultPartitions): SRDD[SciTensor] = {

    val URIs = Source.fromFile(path).mkString.split("\n").toList
    val partitionSize = if (URIs.size > partitions) (URIs.size + partitions) / partitions else 1

    new SRDD[SciTensor](sparkContext, URIs, varName, loadMERGArray(shape, offset), mapNUri(partitionSize))
  }

  /**
   * Constructs an RDD given URI pointing to an HDFS directory of MERG files, a list of variable names,
   * and matrix dimensions. By default the variable name is set to TMP, the dimensions are 9896 x 3298
   * and the value offset is 75.
   *
   * Note that since the files are read from HDFS, the binaryFiles function is used which is called
   * from SparkContext. This is why a normal RDD is returned instead of an SRDD.
   *
   * TODO :: Create an SRDD instead of a normal RDD
   */
  def mergDFSFile(
      path: String,
      varName: List[String] = List("TMP"),
      offset: Double = 75,
      shape: Array[Int] = Array(9896, 3298),
      partitions: Int = defaultPartitions): RDD[SciTensor] = {

    val textFiles = sparkContext.binaryFiles(path, partitions)
    val rdd = textFiles.map(p => {
      val byteArray = p._2.toArray()
      val doubleArray = readMergBytetoJavaArray(byteArray, offset, shape)
      val absT = new BreezeTensor(doubleArray, shape)
      val sciT = new SciTensor("TMP", absT)
      sciT.insertDictionary(("SOURCE", p._1))
      sciT
    })
    rdd
  }

  /**
   * Constructs an SRDD given a nested directories of NetCDF files.
   */
  def openPath(path: String, varName: List[String] = Nil): SRDD[SciTensor] = {
    val datasetPaths = List(path)
    new SRDD[SciTensor](sparkContext, datasetPaths, varName, loadNetCDFNDVar, mapSubFoldersToFolders)
  }

  /**
   * Reads data from WWLLN files into a SQL Dataframe and broadcasts this DF
   * @param WWLLNpath Path on HDFS to the data
   * @param paritions The number of paritions to use
   * return DataFrame with all the WWLLN data at the location provided
   */
  def readWWLLNData(WWLLNpath: String, partitions: Integer): Broadcast[DataFrame] = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._

    val WWLLNdataStrs = sparkContext.textFile(WWLLNpath, partitions).filter(lines => lines.split(",").length != 1)
    val wwllnRDD = WWLLNdataStrs.map(line => line.split(",")).map(p =>
      WWLLN(WWLLNUtils.getWWLLNtimeStr(p(0).trim, p(1).trim), p(2).trim.toDouble, p(3).trim.toDouble,
        p(4).trim.toDouble, p(5).trim.toInt))
    val wwllnDF = wwllnRDD.toDF()
    val broadcastedWWLLN = sparkContext.broadcast(wwllnDF)
    object GetWWLLNDF {
      def getWWLLNDF = broadcastedWWLLN.value
    }
    broadcastedWWLLN
  }

}
