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

import org.slf4j.Logger

/**
 * Created by rahulsp on 7/15/15.
 */
 trait AbstractTensor  extends Serializable {

 type T <: AbstractTensor
  val name : String
  val LOG : Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Reduces the resolution of a DenseMatrix
   * @param blockSize the size of n x n size of blocks.
   * @return
   */
  def reduceResolution (blockSize: Int): T

  def getArray : Unit
  /**
   * Elementwise Operations
   */

  implicit def + (array : T) : T

  implicit def - (array : T) : T

  implicit def *(array : T) : T

  implicit def /(array : T) : T

  implicit def \ (array : T) : T

  /**
   * Linear Algebra Operations
   */

  implicit def **(array : T) : T


  /**
   * In place operations
   */

//  implicit def +=(array : T) : T
//
//  implicit def -=(array : T) : T
//
//  implicit def *=(array : T) : T
//
//  implicit def **=(array : T) : T
//
//  implicit def /=(array : T) : T
//
//  implicit def \=(array : T) : T

  def toString : String


}
