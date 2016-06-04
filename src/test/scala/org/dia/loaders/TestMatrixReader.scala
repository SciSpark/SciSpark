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
package org.dia.loaders

import java.util.Random
import org.nd4j.linalg.factory.Nd4j

/**
 * Generates random matrices.
 */
object TestMatrixReader {

  def loadTestArray(uri: String, varname: String) = {
    val sample = Array(
    Array(240.0, 241.0, 240.0, 241.0, 241.0),
    Array(230.0, 231.0, 240.0, 222.0, 241.0),
    Array(242.0, 243.0, 244.0, 241.0, 232.0),
    Array(240.0, 241.0, 230.0, 231.0, 241.0),
    Array(240.0, 241.0, 240.0, 242.0, 241.0),
    Array(242.0, 243.0, 244.0, 241.0, 242.0))

    val sampleArray = Nd4j.create(sample)

    (sampleArray.data.asDouble, sampleArray.shape)
  }

def loadTestUniformArray(uri: String, varname: String) = {
    val sampleArray = Nd4j.ones(6,5)
    (sampleArray.data.asDouble, sampleArray.shape)
  }
}
