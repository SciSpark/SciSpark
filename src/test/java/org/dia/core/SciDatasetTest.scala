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

import org.scalatest.BeforeAndAfter
import org.scalatest.FunSuite

import org.dia.utils.NetCDFUtils

class SciDatasetTest extends FunSuite with BeforeAndAfter{

  val netcdfDataset = NetCDFUtils.loadNetCDFDataSet("src/test/resources/Netcdf/nc_3B42_daily.2008.01.02.7.bin.nc")
  var Dataset : SciDataset = _

  before {
    Dataset = new SciDataset(netcdfDataset)
  }

  test("testCopy") {
    val copy = Dataset.copy()
    assert(copy == Dataset)
  }

  test("testAttributes") {
    val aerosolAttr = Dataset.attr("FF_GLOBAL.Server")
    assert(aerosolAttr == "DODS FreeFrom based on FFND release 4.2.3")
  }

  test("testVariables") {
    val dataVar = netcdfDataset.findVariable("data")
    val variable = new Variable(dataVar)
    assert(Dataset("data") == variable)
  }

  test("testApply") {
    val dataVar = netcdfDataset.findVariable("data")
    val variable = new Variable(dataVar)
    assert(Dataset("data") == variable)
  }

  test("testInsertAttributes") {
    val attr = "Hi Five!"
    val attrName = "Greeting"
    Dataset.insertAttributes((attrName, attr))
    assert(Dataset.attr(attrName) == attr)
  }

  test("testToString") {
    val string = "nc_3B42_daily.2008.01.02.7.bin.nc\n" +
                 "root group ...\n" +
                 "\tFF_GLOBAL.Server: DODS FreeFrom based on FFND release 4.2.3\n" +
                 "\t_CoordSysBuilder: ucar.nc2.dataset.conv.DefaultConvention\n" +
                 "\tdimensions(sizes): (rows(400), cols(1440))\n" +
                 "\tvariables: (float data(rows, cols))\n"
    assert(Dataset.toString == string)
  }

  test("writeDataset") {
    val name = Dataset.datasetName
    Dataset.write()
    val newDataset = new SciDataset(NetCDFUtils.loadNetCDFDataSet(name))
    assert(newDataset == Dataset)
  }

}
