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

  test("testSplitTiles") {
    val Dataset = new SciDataset(netcdfDataset)
    val someRDD = SparkTestConstants.sc.sparkContext.parallelize(0 until 1)
    val keyFunc : SciDataset => Int = sciD => sciD.datasetName.toInt

    // sqaure subset
    LOG.info("Square subset of shape (80,80)")
    val subSettedRDD = someRDD.map(p => Dataset.setName(p.toString))
      .splitTiles("data", keyFunc, 80, 80)
    val subsets = subSettedRDD.collect
    for ((ranges, (index, tensor)) <- subsets) {
      assert(Dataset("data")(ranges: _*) == tensor)
    }

    // rectangle subset
    LOG.info("Rectangle subset")
    val subSettedRDD2 = someRDD.map(p => Dataset.setName(p.toString))
      .splitTiles("data", keyFunc, 200, 30)
    val subsets2 = subSettedRDD2.collect
    for ((ranges, (index, tensor)) <- subsets2) {
      assert(Dataset("data")(ranges: _*) == tensor)
    }

    // row subset
    LOG.info("Row subset")
    val subSettedRDD3 = someRDD.map(p => Dataset.setName(p.toString))
      .splitTiles("data", keyFunc, 100)
    val subsets3 = subSettedRDD3.collect
    for ((ranges, (index, tensor)) <- subsets3) {
      assert(Dataset("data")(ranges: _*) == tensor)
    }

    // col subset
    LOG.info("Col subset")
    val subSettedRDD4 = someRDD.map(p => Dataset.setName(p.toString))
      .splitTiles("data", keyFunc, 400, 1)
    val subsets4 = subSettedRDD4.collect
    for ((ranges, (index, tensor)) <- subsets3) {
      assert(Dataset("data")(ranges: _*) == tensor)
    }

  }

  test("testRepartitionBySpace") {
    /**
     * Create a list of Datasets each with its values incremented
     * by a different amount
     */
    val Dataset = new SciDataset(netcdfDataset)
    val lenAlongDimension = 10
    val sortedDatasets = (0 until lenAlongDimension).map(p => {
      val d = Dataset.copy()
      d.setName(p.toString)
      d("data") += p
      d
    })

    val someRDD = SparkTestConstants.sc.sparkContext.parallelize(sortedDatasets)
    val keyFunc : SciDataset => Int = sciD => sciD.datasetName.toInt

    // Join together entire arrays without subsetting
    LOG.info("Testing join of entire array")
    val spacePartitionedRDD = someRDD.repartitionBySpace("data", keyFunc)
    val spacePartitioned = spacePartitionedRDD.collect
    for((abst, indx) <- spacePartitioned.zipWithIndex) {
      assert(abst.shape().toList == List(lenAlongDimension, 400, 1440))
      for(i <- 0 until lenAlongDimension) {
        val k = abst(i)
        val z = sortedDatasets(i)("data")
        assert(k() == z())
      }
    }
    LOG.info("Passed")

    // Split into shapes of 200 by 1440
    LOG.info("Testing tile by 200 x 1440")
    val spacePartitionedRDD2 = someRDD.repartitionBySpace("data", keyFunc, 200)
    val spacePartitioned2 = spacePartitionedRDD2.collect.sortBy(p => p.name)
    for((abst, indx) <- spacePartitioned2.zipWithIndex) {
      assert(abst.shape().toList == List(lenAlongDimension, 200, 1440))
      for(i <- 0 until lenAlongDimension) {
        val k = abst(i)
        val z = sortedDatasets(i)("data")((200 * indx, 200 * (indx + 1)))
        assert(k() == z())
      }
    }

    // Split into shapes of 200 by 144
    LOG.info("Testing tile by 200 x 144")
    val spacePartitionedRDD3 = someRDD.repartitionBySpace("data", keyFunc, 200, 144)
    val pattern = "\\[([0-9]+):([0-9]+)\\,([0-9]+):([0-9]+)\\]".r
    val spacePartitioned3 = spacePartitionedRDD3.collect.sortBy(p => {
      val pattern(w, x, y, z) = p.name.slice(13, p.name.length)
      ((w.toInt, x.toInt), (y.toInt, z.toInt))
    })
    for((abst, indx) <- spacePartitioned3.zipWithIndex) {
      assert(abst.shape().toList == List(lenAlongDimension, 200, 144))
      val rangeList = List((200 * (indx / 10), 200 * ((indx / 10) + 1)), (144 * (indx % 10), 144 * ((indx % 10) + 1)))
      for(i <- 0 until lenAlongDimension) {
        val k = abst(i)
        val z = sortedDatasets(i)("data")(rangeList: _*)
        assert(k() == z())
      }
    }
  }


}
