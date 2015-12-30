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

import org.dia.testenv.SparkTestConstants
import org.dia.loaders.NetCDFReader._
import org.dia.partitioners.STrmmPartitioner._
import org.dia.urlgenerators.HourlyTrmmURLGenerator;
import org.scalatest.FunSuite

/**
 * Tests for proper construction of SRDD's from URIs.
 */
class SRDDTest extends FunSuite {

  val sc = SparkTestConstants.sc
  val testLinks = SparkTestConstants.datasetPath

  test("GroupingByYearPartitioning") {
    val urls = HourlyTrmmURLGenerator.generateTrmmDaily(1999, 2000).toList.slice(0, 2)
    sc.setLocalProperty("log4j.configuration", "resources/log4j-defaults.properties")
    val sRDD = new SRDD[SciTensor](sc.sparkContext, urls, List("precipitation"), loadNetCDFNDVar, mapOneYearToManyTensorTRMM)
    val Tensor = sRDD.collect()(0)
    assert(true)
  }

  test("GroupingByDayPartitioning") {
    val urls = HourlyTrmmURLGenerator.generateTrmmDaily(1999, 2000).toList.slice(0, 2)
    sc.setLocalProperty("log4j.configuration", "resources/log4j-defaults.properties")
    val sRDD = new SRDD[SciTensor](sc.sparkContext, urls, List("precipitation"), loadNetCDFNDVar, mapOneDayToManyTensorTRMM)
    val Tensor = sRDD.collect()(0)
    assert(true)
  }

}
