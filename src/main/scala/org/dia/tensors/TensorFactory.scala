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

import org.dia.Constants
import org.dia.Constants.{BREEZE_LIB, ND4J_LIB}
import scala.collection.mutable

/**
 * Factory to create tensors.
 */
object TensorFactory {
  
  /**
   * Creates specific tensor
   * @param arrayLib
   * @param loadFunc
   * @return Breeze/Nd4j tensor depending on chosen library.
   */
  def getTensor[T <: AbstractTensor](arrayLib: String, loadFunc: () => (Array[Double], Array[Int])): AbstractTensor = {
    arrayLib match {
      case BREEZE_LIB => return new BreezeTensor(loadFunc)
      case ND4J_LIB => return new Nd4jTensor(loadFunc)
    }
  }

}
