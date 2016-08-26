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

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.dia.core.SRDDFunctions._
import org.dia.testenv.SparkTestConstants
import org.dia.utils.NetCDFUtils

class SRDDFunctionsTest extends FunSuite with BeforeAndAfterEach {

  val netcdfDataset = NetCDFUtils.loadNetCDFDataSet("src/test/resources/Netcdf/nc_3B42_daily.2008.01.02.7.bin.nc")
  val directoryString = "src/test/resources/Output/"
  val directory = new java.io.File(directoryString)

  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)

  override def beforeEach {
    FileUtils.cleanDirectory(directory)
  }

  override def afterEach {
    FileUtils.cleanDirectory(directory)
  }

  test("testWriteSRDD") {
    val Dataset = new SciDataset(netcdfDataset)
    val someRDD = SparkTestConstants.sc.sparkContext.parallelize(0 until 10)
    someRDD.map(p => {
      Dataset.setName("AnotherFile_" + p + ".nc")
    }).writeSRDD(directoryString)

    val fileCount = directory.listFiles.count(p => p.getName.charAt(0) != '.')
    assert(fileCount == 10)
  }

  test("testRepartitionBySubset") {
    val Dataset = new SciDataset(netcdfDataset)
    val someRDD = SparkTestConstants.sc.sparkContext.parallelize(0 until 1)
    val keyFunc : SciDataset => Int = sciD => sciD.datasetName.toInt

    // sqaure subset
    LOG.info("Square subset of shape (2,2)")
    val subSettedRDD = someRDD.map(p => Dataset.setName(p.toString))
      .splitBySubset("data", keyFunc, 2, 2)
    val subsets = subSettedRDD.collect
    for ((ranges, index, tensor) <- subsets) {
      assert(Dataset("data")()(ranges: _*).copy == tensor)
    }

    // rectangle subset
    LOG.info("Rectangle subset")
    val subSettedRDD2 = someRDD.map(p => Dataset.setName(p.toString))
      .splitBySubset("data", keyFunc, 2, 3)
    val subsets2 = subSettedRDD2.collect
    for ((ranges, index, tensor) <- subsets2) {
      assert(Dataset("data")()(ranges: _*).copy == tensor)
    }

    // row subset
    LOG.info("Row subset")
    val subSettedRDD3 = someRDD.map(p => Dataset.setName(p.toString))
      .splitBySubset("data", keyFunc, 1)
    val subsets3 = subSettedRDD3.collect
    for ((ranges, index, tensor) <- subsets3) {
      assert(Dataset("data")()(ranges: _*).copy == tensor)
    }

    // col subset
    LOG.info("Col subset")
    val subSettedRDD4 = someRDD.map(p => Dataset.setName(p.toString))
      .splitBySubset("data", keyFunc, 1)
    val subsets4 = subSettedRDD4.collect
    for ((ranges, index, tensor) <- subsets3) {
      assert(Dataset("data")()(ranges: _*).copy == tensor)
    }
  }


}
