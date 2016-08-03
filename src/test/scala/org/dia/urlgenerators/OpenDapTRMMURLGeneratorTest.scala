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
package org.dia.urlgenerators

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfter, FunSuite, Ignore}

/**
 * Tests whether the OpenDapTRMM URLs creator works.
 * NB the parameter hdfsURL is passed on the command line via -DhdfsURL=hdfs://<hostname>:<port>
 */
@Ignore
class OpenDapTRMMURLGeneratorTest extends FunSuite with BeforeAndAfter {
  var hdfsURL: String = null
  val hadoopConf = new Configuration()
  var hdfs: FileSystem = null

  before {
    hdfsURL = sys.props("hdfsURL")
    hadoopConf.set("fs.defaultFS", hdfsURL)
    hdfs = FileSystem.get(hadoopConf)
  }


  test("testLinkGenerationDaily") {
    val checkLink = false
    val vars = List("precipitation")
    OpenDapTRMMURLGenerator.run(checkLink, hdfsURL, "testLinkfileD.txt", "200101010000", "200101310000", 1, vars)
    if (hdfs.exists(new Path("testLinkfileD.txt"))) {
      assert(true)
    } else {
      assert(false)
    }
  }

  test("testLinkGeneration3Hrly") {
    val checkLink = false
    val vars = List("precipitation")
    OpenDapTRMMURLGenerator.run(checkLink, hdfsURL, "testLinkfileH.txt", "201001010000", "201001031500", 2, vars)
    if (hdfs.exists(new Path("testLinkfileH.txt"))) {
      assert(true)
    } else {
      assert(false)
    }
  }

  test("testLinkGenerationDailyWithSelection") {
    val checkLink = false
    val vardims = List("data,1,399,1,1439")
    OpenDapTRMMURLGenerator.run(checkLink, hdfsURL, "testLinkfileDsub.txt", "200103010000", "200103310000", 1, vardims)
    if (hdfs.exists(new Path("testLinkfileDsub.txt"))) {
      assert(true)
    } else {
      assert(false)
    }
  }

  test("testLinkGeneration3HrlyWithSelection") {
    val checkLink = true
    val vardims = List("precipitation,1,1439,1,399", "nlon,1,1439", "nlat,1,399")
    OpenDapTRMMURLGenerator.run(checkLink, hdfsURL, "testLinkfileHsub.txt", "201006150000", "201006161500", 2, vardims)
    if (hdfs.exists(new Path("testLinkfileHsub.txt"))) {
      assert(true)
    } else {
      assert(false)
    }
  }

}
