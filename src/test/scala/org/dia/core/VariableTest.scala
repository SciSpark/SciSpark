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

import org.dia.tensors.Nd4jTensor
import org.dia.utils.NetCDFUtils
import org.scalatest.FunSuite

class VariableTest extends FunSuite {


  val netcdfDataset = NetCDFUtils.loadNetCDFDataSet("resources/merg/merg_2006091100_4km-pixel.nc")
  val name = "ch4"

  val (array, shape) = NetCDFUtils.netCDFArrayAndShape(netcdfDataset, name)
  val dataVar = netcdfDataset.findVariable(name)

  val tensor = new Nd4jTensor(array, shape)
  val variable = new Variable(dataVar)


  test("testCopy") {
    val copy = variable.copy()
    assert(copy == variable)
  }

  test("testAttributesApply") {
    val gridTypeAttribute = variable("grid_type")
    assert(gridTypeAttribute == "linear")
  }

  test("testApply") {
    val tensorCopy = tensor.copy
    val origTensor = variable()
    assert(tensor == origTensor)
  }

  test("testInsertAttributes") {
    variable.insertAttributes(("random_attribute", "value1"))
    val insertedAttribute = variable("random_attribute")
    assert(insertedAttribute == "value1")
  }

  test("testShape") {
    val shape = variable.shape()
    assert(shape.toList == List(1238, 4125))
  }

  test("testData") {
    val shapeProduct = variable.shape().product
    val array = variable.data()
    assert(array.length == shapeProduct)
    assert(array.deep == this.array.deep)
  }

  test("testToString") {
    val string = "float ch4\n" +
                "\trandom_attribute: value1\n" +
                "\tlevel_description: Earth surface\n" +
                "\tgrid_type: linear\n" +
                "\tmissing_value: 330.0\n" +
                "\tunits: \n" +
                "\tlong_name: IR BT (add 75 to this value)\n" +
                "\tgrid_name: grid01\n" +
                "\tcomments: Unknown1 variable comment\n" +
                "\ttime_statistic: instantaneous\n" +
                "current shape = (1238, 4125)\n"
    assert(variable.toString == string)
  }

}
