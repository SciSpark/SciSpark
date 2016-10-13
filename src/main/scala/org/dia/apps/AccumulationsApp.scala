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
package org.dia.apps

import java.io.{ File, PrintWriter }

import scala.collection.mutable
import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.rdd.RDD

import org.dia.core.{SciDataset, SciSparkContext, SRDDFunctions, Variable}
import org.dia.urlgenerators.OpenDapTRMMURLGenerator
import org.dia.utils.{FileUtils, NetCDFUtils}

object AccumulationsApp extends App {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  val masterURL = if (args.isEmpty) "local[*]" else args(0)
  val partitions = if (args.length <= 1) 8 else args(1).toInt
  val hdfsURL = if (args.length <= 2) "hdfs://localhost:9000" else args(2)
  val varname = if (args.length <= 3) "precipitation" else args(3)
  val outputLoc = if (args.length <= 4) "output" else args(4)
  val credentialsFilePath = "src/test/resources/TestHTTPCredentials"
  val fileSepStr = System.getProperty("file.separator")
  val credentialList = Source.fromFile(credentialsFilePath)
    .getLines()
    .map(p => {
      val split = p.split("\\s+")
      (split(0), split(1))
    }).toList
  val username = credentialList(0)._2
  val password = credentialList(1)._2

  val outputDir = FileUtils.checkHDFSWrite(outputLoc)

  val hdfsDir = outputDir + fileSepStr + "TRMM3B42.txt"
  val ugi = UserGroupInformation.getCurrentUser().getUserName()
  val hdfsPath = hdfsURL + fileSepStr + "user" + fileSepStr + ugi + fileSepStr +
    outputDir + fileSepStr

  logger.info("Starting Simple Accumulation")

  val sc = new SciSparkContext(masterURL, "SciSpark example: Simple Accumulation")

  sc.addHTTPCredential("http://disc2.nascom.nasa.gov/opendap/hyrax/TRMM_L3/", username, password)
  /**
   * The data is 3hrly TRMM data. See
   * http://disc.sci.gsfc.nasa.gov/precipitation/documentation/TRMM_README/TRMM_3B42_readme.shtml
   * for more details
   */
  OpenDapTRMMURLGenerator.run(false, hdfsURL, hdfsDir, "201006150000", "201006151200", 2,
    List("precipitation,1,1439,1,399", "nlon,1,1439", "nlat,1,399"))

  val sRDD = sc.sciDatasets(hdfsDir, List(varname), partitions)

  /**
   * Add the frame as an attribute to the SciDataset
   */
  val labels = sRDD.map(p => {
    val splitName = p.datasetName.split("\\.")
    val FrameID = splitName(1) + splitName(2)
    p("FRAME") = FrameID
  })
  /**
   * Convert the data from hrly rate every 3 hrs to 3hrly accumulations.
   */
  val accu3hrly = labels.map(p => {
    /* Verbose way to add a new Variable to a sciDataset */
    val precipAccStr = "(" + varname + " * 3.0)"
    val precipAccTensor = p(varname)() * 3.0
    val precipAccVar = new Variable("precip3hrlyAcc", "float", precipAccTensor.data,
      p(varname).shape(), List(("Calculation", precipAccStr)), p(varname).dims)
    precipAccVar.insertAttributes(("long_name", "precipitation 3hrlyAccumulation"))
    precipAccVar.insertAttributes(("units", "mm"))
    p.insertVariable(("precip3hrlyAcc", precipAccVar))
  })

  /**
   * To find 6hrly accumulation, first the time must be maintained.
   */
  val consecutiveTimes = SRDDFunctions.fromRDD(accu3hrly).pairConsecutiveFrames("FRAME")

  /**
   * Sum consecutive frames for 6hrly accumulation
   */
  val accu6hrly = consecutiveTimes.map(p => {
    /* Concise way to add a new Variable to the sciDataset using the update function */
    val summationStr = "(precip3hrlyAcc._1 + precip3hrlyAcc._2)"
    p._1.update("6hrlyaccu", p._1("precip3hrlyAcc") + p._2("precip3hrlyAcc"))
    p._1("6hrlyaccu").insertAttributes(("Calculation", summationStr))
    p._1("6hrlyaccu").insertAttributes(("long_name", "precipitation 6hrlyAccumulation"))
    p._1("6hrlyaccu").insertAttributes(("units", "mm"))

    /**
     * Update the sciDataset name to reflect the computation
     */
    val splitName = p._1.datasetName.split("\\.")
    val newname = "6hrly-accu-starting-at" + splitName(1) + splitName(2) + ".nc"
    p._1.setName(newname)
    p._1
  })

  /**
   * Write the sRDD to netCDF files
   */
  SRDDFunctions.fromRDD(accu6hrly).writeSRDD(hdfsPath, "/tmp/")
  logger.info("Successfully completed. See " + hdfsPath + "for the output netCDF files.")
}
