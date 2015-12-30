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
 * An abstract tensor
 */
trait AbstractTensor extends Serializable with SliceableArray {

  type T <: AbstractTensor
  val name: String
  val LOG = org.slf4j.LoggerFactory.getLogger(this.getClass)

  def zeros(shape: Int*): T

  def map(f: Double => Double): AbstractTensor

  /**
   * Indexed Operations
   */

  def put(value: Double, shape: Int*): Unit

  /**
   * Element-wise Operations
   */

  def +(array: AbstractTensor): T

  def -(array: AbstractTensor): T

  def *(array: AbstractTensor): T

  def /(array: AbstractTensor): T

  def \(array: AbstractTensor): T

  /**
   * Linear Algebra Operations
   */

  def **(array: AbstractTensor): T

  /**
   * Masking operations
   */

  def <=(num: Double): T
  def :=(num: Double): T

  /**
   * Utility Methods
   */

  def cumsum: Double
  def toString: String

  override def equals(any: Any): Boolean = {
    val array = any.asInstanceOf[AbstractTensor]
    if (array.rows != this.rows) return false
    if (array.cols != this.cols) return false
    for (row <- 0 to array.rows - 1) {
      for (col <- 0 to array.cols - 1) {
        if (array(row, col) != this(row, col)) return false
      }
    }
    true
  }

  def shape: Array[Int]

  def isZero: Boolean
  /**
   *  Shortcut test whether tensor is zero
   *  in case we know its entries are all non-negative.
   */
  def isZeroShortcut: Boolean
  def max: Double
  def min: Double

}
