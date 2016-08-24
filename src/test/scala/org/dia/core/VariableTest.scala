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

import scala.collection.JavaConverters._

import org.scalatest.{BeforeAndAfterEach, FunSuite}

import org.dia.algorithms.mcc.MCCOps
import org.dia.tensors.{AbstractTensor, Nd4jTensor}
import org.dia.utils.NetCDFUtils

class VariableTest extends FunSuite with BeforeAndAfterEach {

  val netcdfDataset = NetCDFUtils.loadNetCDFDataSet("src/test/resources/CSSM/sresa1b_ncar_ccsm3-example.nc")
  val name = "tas"
  val (array, shape) = NetCDFUtils.netCDFArrayAndShape(netcdfDataset, name)
  var tensor = new Nd4jTensor(array, shape)
  val dataVar = netcdfDataset.findVariable(name)
  var variable = new Variable(dataVar)

  var leftName : String = _
  var rightName : String = _
  var leftTensor : AbstractTensor = _
  var rightTensor : AbstractTensor = _
  var leftVar : Variable = _
  var rightVar : Variable = _
  var rightScalar : Double = _

  override def beforeEach(): Unit = {
    leftName = "left"
    rightName = "right"
    leftTensor = new Nd4jTensor((0d until 16d by 1d).toArray, Array(4, 4))
    rightTensor = new Nd4jTensor((16d until 32d by 1d).toArray, Array(4, 4))
    leftVar = new Variable(leftName, leftTensor)
    rightVar = new Variable(rightName, rightTensor)
    rightScalar = 5.0
  }

  test("testCopy") {
    val copy = variable.copy()
    assert(copy == variable)
  }

  test("test Clone") {
    val clone = variable.clone()
    assert(clone == variable)
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

  test("singleIndexApply") {
    val solutionName = leftName + "[0]"
    val indexSliceVar = leftVar(0)
    val solution = new Variable(solutionName, leftTensor(0))
    assert(indexSliceVar == solution)
  }

  test("multipleIndexApply") {
    val solutionName = leftName + "[2:3,1:4]"
    val indexSliceVar = leftVar((2, 3), (1, 4))
    val solution = new Variable(solutionName, leftTensor((2, 3), (1, 4)))
    assert(indexSliceVar == solution)
  }

  test("testInsertAttributes") {
    val copy = variable.copy()
    copy.insertAttributes(("random_attribute", "value1"))
    val insertedAttribute = copy("random_attribute")
    assert(insertedAttribute == "value1")
  }

  test("test Dims") {
    val dims = dataVar.getDimensions.asScala.map(p => (p.getFullName, p.getLength))
    val varDims = variable.dims.toMap
    for((dim, len) <- dims) assert(varDims(dim) == len)
  }

  test("testShape") {
    val shape = variable.shape()
    assert(shape.toList == List(1, 128, 256))
  }

  test("testData") {
    val shapeProduct = variable.shape().product
    val (array, shape) = NetCDFUtils.netCDFArrayAndShape(netcdfDataset, name)
    assert(array.length == shapeProduct)
    assert(array.toList == variable.data().toList)
  }

  test("testToString") {
    val string = "float tas(time, lat, lon)\n" +
      "\t_FillValue: 1.0E20\n" +
      "\tcell_method: time: mean\n" +
      "\tcell_methods: time: mean (interval: 1 month)\n" +
      "\tcomment: Created using NCL code CCSM_atmm_2cf.ncl on\n" +
      " machine eagle163s\n" +
      "\tcoordinates: height\n" +
      "\thistory: Added height coordinate\n" +
      "\tlong_name: air_temperature\n" +
      "\tmissing_value: 1.0E20\n" +
      "\toriginal_name: TREFHT\n" +
      "\toriginal_units: K\n" +
      "\tstandard_name: air_temperature\n" +
      "\tunits: K\n" +
      "current shape = (1, 128, 256)\n"
    assert(variable.toString == string)
  }

  test("test += tensor") {
    val solutionName = "(" + leftName + " += " + rightName + ")"
    val solution = new Variable(solutionName, leftTensor += rightTensor)
    leftVar += rightVar
    assert(leftVar == solution)
  }

  test("test += scalar") {
    val solutionName = "(" + leftName + " += " + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor += rightScalar)
    leftVar += rightScalar
    assert(leftVar == solution)
  }

  test("test + tensor") {
    val solutionName = "(" + leftName + " + " + rightName + ")"
    val solution = new Variable(solutionName, leftTensor + rightTensor)
    val leftOpRight = leftVar + rightVar
    assert(leftOpRight == solution)
  }

  test("test + scalar") {
    val solutionName = "(" + leftName + " + " + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor + rightScalar)
    val leftOpRight = leftVar + rightScalar
    assert(leftOpRight == solution)
  }

  test("test -= tensor") {
    val solutionName = "(" + leftName + " -= " + rightName + ")"
    val solution = new Variable(solutionName, leftTensor -= rightTensor)
    leftVar -= rightVar
    assert(leftVar == solution)
  }

  test("test -= scalar") {
    val solutionName = "(" + leftName + " -= " + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor -= rightScalar)
    leftVar -= rightScalar
    assert(leftVar == solution)
  }

  test("test - tensor") {
    val solutionName = "(" + leftName + " - " + rightName + ")"
    val solution = new Variable(solutionName, leftTensor - rightTensor)
    val leftOpRight = leftVar - rightVar
    assert(leftOpRight == solution)
  }

  test("test - scalar") {
    val solutionName = "(" + leftName + " - " + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor - rightScalar)
    val leftOpRight = leftVar - rightScalar
    assert(leftOpRight == solution)
  }

  test("test *= tensor") {
    val solutionName = "(" + leftName + " *= " + rightName + ")"
    val solution = new Variable(solutionName, leftTensor *= rightTensor)
    leftVar *= rightVar
    assert(leftVar == solution)
  }

  test("test *= scalar") {
    val solutionName = "(" + leftName + " *= " + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor *= rightScalar)
    leftVar *= rightScalar
    assert(leftVar == solution)
  }

  test("test * tensor") {
    val solutionName = "(" + leftName + " * " + rightName + ")"
    val solution = new Variable(solutionName, leftTensor * rightTensor)
    val leftOpRight = leftVar * rightVar
    assert(leftOpRight == solution)
  }

  test("test * scalar") {
    val solutionName = "(" + leftName + " * " + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor * rightScalar)
    val leftOpRight = leftVar * rightScalar
    assert(leftOpRight == solution)
  }

  test("test /= tensor") {
    val solutionName = "(" + leftName + " /= " + rightName + ")"
    val solution = new Variable(solutionName, leftTensor /= rightTensor)
    leftVar /= rightVar
    assert(leftVar == solution)
  }

  test("test /= scalar") {
    val solutionName = "(" + leftName + " /= " + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor /= rightScalar)
    leftVar /= rightScalar
    assert(leftVar == solution)
  }

  test("test / tensor") {
    val solutionName = "(" + leftName + " / " + rightName + ")"
    val solution = new Variable(solutionName, leftTensor / rightTensor)
    val leftOpRight = leftVar / rightVar
    assert(leftOpRight == solution)
  }

  test("test / scalar") {
    val solutionName = "(" + leftName + " / " + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor / rightScalar)
    val leftOpRight = leftVar / rightScalar
    assert(leftOpRight == solution)
  }

  test("test matrixMul(**) tensor") {
    val solutionName = "(" + leftName + " ** " + rightName + ")"
    val solution = new Variable(solutionName, leftTensor ** rightTensor)
    val leftOpRight = leftVar ** rightVar
    assert(leftOpRight == solution)
  }

  test("test mask") {
    val solutionName = "maskf(" + leftName + "," + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor.mask(_ < 3, rightScalar))
    val leftOpRight = leftVar.mask(_ < 3, rightScalar)
    assert(leftOpRight == solution)
  }

  test("test > scalar") {
    val solutionName = "(" + leftName + " > " + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor > rightScalar)
    val leftOpRight = leftVar > rightScalar
    assert(leftOpRight == solution)
  }

  test("test >= scalar") {
    val solutionName = "(" + leftName + " >= " + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor >= rightScalar)
    val leftOpRight = leftVar >= rightScalar
    assert(leftOpRight == solution)
  }

  test("test < scalar") {
    val solutionName = "(" + leftName + " < " + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor < rightScalar)
    val leftOpRight = leftVar < rightScalar
    assert(leftOpRight == solution)
  }

  test("test <= scalar") {
    val solutionName = "(" + leftName + " <= " + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor <= rightScalar)
    val leftOpRight = leftVar <= rightScalar
    assert(leftOpRight == solution)
  }

  test("test := scalar") {
    val solutionName = "(" + leftName + " := " + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor := rightScalar)
    val leftOpRight = leftVar := rightScalar
    assert(leftOpRight == solution)
  }

  test("test != scalar") {
    val solutionName = "(" + leftName + " != " + rightScalar + ")"
    val solution = new Variable(solutionName, leftTensor != rightScalar)
    val leftOpRight = leftVar != rightScalar
    assert(leftOpRight == solution)
  }

  test("test mean") {
    val axis = Array(0)
    val solutionName = "mean(" + leftName + "," + axis.toList + ")"
    val solution = new Variable(solutionName, leftTensor.mean(axis: _*))
    val leftOpRight = leftVar.mean(axis)
    assert(leftOpRight == solution)
  }

  test("test ReduceRectangleResolution") {
    val (row, col) = (2, 2)
    val solutionName = "reduceResolution(" + leftName + "," + (row, col) + ")"
    val solution = new Variable(solutionName, MCCOps.reduceRectangleResolution(leftTensor, row, col))
    val leftOpRight = leftVar.reduceRectangleResolution(row, col)
    assert(leftOpRight == solution)
  }

  test("test ReduceResolution") {
    val block = 2
    val solutionName = "reduceResolution(" + leftName + "," + (block, block) + ")"
    val solution = new Variable(solutionName, MCCOps.reduceResolution(leftTensor, block))
    val leftOpRight = leftVar.reduceResolution(block)
    assert(leftOpRight == solution)
  }

  test("test Skew") {
    val axis = Array(0)
    val solutionName = "skew(" + leftName + "," + axis.toList + ")"
    val solution = new Variable(solutionName, leftTensor.skew(axis: _*))
    val leftOpRight = leftVar.skew(axis)
    assert(leftOpRight == solution)
  }

  test("test Detrend") {
    val axis = Array(0)
    val solutionName = "detrend(" + leftName + "," + axis.toList + ")"
    val solution = new Variable(solutionName, leftTensor.detrend(axis(0)))
    val leftOpRight = leftVar.detrend(axis)

    println(solution())
    println(leftOpRight())
    assert(leftOpRight == solution)
  }

  test("test Broadcast") {
    val axis = Array(3, 4, 4)
    val solutionName = "broadcast(" + leftName + "," + axis.toList + ")"
    val solution = new Variable(solutionName, leftTensor.broadcast(axis))
    val leftOpRight = leftVar.broadcast(axis)
    assert(leftOpRight == solution)
  }

  test("test Std") {
    val axis = Array(0)
    val solutionName = "std(" + leftName + "," + axis.toList + ")"
    val solution = new Variable(solutionName, leftTensor.std(axis(0)))
    val leftOpRight = leftVar.std(axis)
    assert(leftOpRight == solution)
  }

}
