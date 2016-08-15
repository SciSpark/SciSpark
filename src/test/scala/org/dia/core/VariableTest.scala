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

import org.scalatest.FunSuite

import org.dia.tensors.Nd4jTensor
import org.dia.utils.NetCDFUtils

class VariableTest extends FunSuite {


  val netcdfDataset = NetCDFUtils.loadNetCDFDataSet("src/test/resources/CSSM/sresa1b_ncar_ccsm3-example.nc")
  val name = "tas"

  val (array, shape) = NetCDFUtils.netCDFArrayAndShape(netcdfDataset, name)
  val dataVar = netcdfDataset.findVariable(name)

  val tensor = new Nd4jTensor(array, shape)
  val variable = new Variable(dataVar)


  test("testCopy") {
    val copy = variable.copy()
    assert(copy == variable)
  }

  test("testAttributesApply") {
    val gridTypeAttribute = variable("history")
    assert(gridTypeAttribute == "Added height coordinate")
  }

  test("testApply") {
    val tensorCopy = tensor.copy
    val origTensor = variable()
    assert(tensorCopy == origTensor)
  }

  test("testInsertAttributes") {
    val copy = variable.copy()
    copy.insertAttributes(("random_attribute", "value1"))
    val insertedAttribute = copy("random_attribute")
    assert(insertedAttribute == "value1")
  }

  test("testShape") {
    val shape = variable.shape()
    assert(shape.toList == List(1, 128, 256))
  }

  test("testData") {
    val shapeProduct = variable.shape().product
    val array = variable.data()
    assert(array.length == shapeProduct)
    assert(array.deep == this.array.deep)
  }

  test("testToString") {
    val string = "float tas(time, lat, lon)\n" +
                "\tcomment: Created using NCL code CCSM_atmm_2cf.ncl on\n" +
                " machine eagle163s\n" +
                "\tmissing_value: 1.0E20\n" +
                "\t_FillValue: 1.0E20\n" +
                "\tcell_methods: time: mean (interval: 1 month)\n" +
                "\thistory: Added height coordinate\n" +
                "\tcoordinates: height\n" +
                "\toriginal_units: K\n" +
                "\toriginal_name: TREFHT\n" +
                "\tstandard_name: air_temperature\n" +
                "\tunits: K\n" +
                "\tlong_name: air_temperature\n" +
                "\tcell_method: time: mean\n" +
                "current shape = (1, 128, 256)\n"
    assert(variable.toString == string)
  }

}
