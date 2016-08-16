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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.nd4j.linalg.factory.Nd4j
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.dia.core.SciTensor
import org.dia.utils.NetCDFUtils

class SciTensorTest extends FunSuite with BeforeAndAfterEach{

  var absTCopy : AbstractTensor = _
  var sciT : SciTensor = _

  override def beforeEach(): Unit = {
    val square = Nd4j.create((0d to 9d by 1d).toArray, Array(3, 3))
    val absT = new Nd4jTensor(square)
    absTCopy = absT.copy
    sciT = new SciTensor("sample", absT)
  }

  test("ApplyNullary") {
    val extracted = sciT("sample")()
    assert(extracted.isInstanceOf[org.dia.tensors.AbstractTensor])
    assert(extracted == absTCopy)
  }

  test("writeToNetCDF") {
    sciT.writeToNetCDF("TestSciTensor.nc")
    val newDataset = NetCDFUtils.loadNetCDFDataSet("TestSciTensor.nc")
    val vars = newDataset.getVariables.asScala.map(p => p.getFullName)
    val arrayShapePairs = vars.map(p => (p, NetCDFUtils.netCDFArrayAndShape(newDataset, p)))

    val tensors = arrayShapePairs.map({ case(name, (arr, shp)) => (name, new Nd4jTensor(arr, shp))})
    val sciTNew = new SciTensor(new mutable.HashMap[String, AbstractTensor]() ++= tensors)
    assert(sciTNew == sciT)
  }
}
