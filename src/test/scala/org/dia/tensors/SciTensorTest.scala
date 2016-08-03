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

import org.nd4j.linalg.factory.Nd4j
import org.scalatest.FunSuite

import org.dia.core.SciTensor

class SciTensorTest extends FunSuite {

  test("ApplyNullary") {
    val square = Nd4j.create((0 to 9).toArray, Array(3, 3))
    val absT = new Nd4jTensor(square)
    val absTCopy = absT.copy
    val sciT = new SciTensor("sample", absT)
    val extracted = sciT("sample")()
    assert(extracted.isInstanceOf[org.dia.tensors.AbstractTensor])
    assert(extracted == absTCopy)
  }
}
