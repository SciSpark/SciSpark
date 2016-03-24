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

import scala.io.Source

/**
 * Utility functions to read binary data from MERG files.
 * Note that MERG files by default store floating point numbers
 * in unsigned bytes.
 *
 * Note that 3 copies of the array are created in memory with these calls,
 * primarily due to the conversion from byte arrays to Double arrays.
 * 1) The file or byte array
 * 2) The slice of the array needed, since MERG files 30 minute intervals in hourly files.
 * 3) The mapped conversion to Doubles
 *
 * 2n + 8n = 10n where n corresponds to the number of bytes.
 * The last 8n is due to the double conversion.
 */
object MergReader {

  /**
   * Returns a Java 1-dimensional array loaded from a shape array at a specified offset
   *
   * @param shape a shape integer array representing the MERG shape
   * @param offset an offset within the array to start reading from
   * @param file a file path pointing to the MERG file
   * @param varName a variable name to extract
   * @return
   */
  def loadMERGArray(shape: Array[Int], offset: Double)(file: String, varName: String): (Array[Double], Array[Int]) = {
    val java1dArray = readMergtoJavaArray(file, offset, shape)
    (java1dArray, shape)
  }

  /**
   * Returns a Java 1-dimensional array loaded from a MERG file path at a specified offset
   *
   * @param file a file path pointing to the MERG file
   * @param shape a shape integer array representing the MERG shape
   * @param offset an offset within the array to start reading from
   * @return
   */
  def loadMERGArray(file: String, shape: Array[Int], offset: Double): (Array[Double], Array[Int]) = {
    val java1dArray = readMergtoJavaArray(file, offset, shape)
    (java1dArray, shape)
  }

  /**
   * Returns a Java array loaded from a MERG file at a specified offset, with a specified shape
   *
   * @param file a file path pointing to the MERG file
   * @param offset an offset within the array to start reading from
   * @param shape a shape integer array representing the MERG shape
   * @return
   */
  def readMergtoJavaArray(file: String, offset: Double, shape: Array[Int]): Array[Double] = {
    val Sourcefile = Source.fromFile(file, "ISO8859-1")
    val numElems = shape.product
    val byteArray = Sourcefile.map(_.toInt).slice(0, numElems).toArray
    Sourcefile.close()
    val SourceArray = byteArray.map(floatByte => (floatByte & 0xff).asInstanceOf[Float].toDouble + offset)
    SourceArray
  }

  /**
   * Returns a Java array loaded from a MERG byte array at a specified offset based on a specified shape
   *
   * @param byteArray a byte array containing bytes from a MERG file
   * @param offset an offset within the array to start reading from
   * @param shape a shape integer array representing the MERG shape
   * @return
   */
  def readMergBytetoJavaArray(byteArray: Array[Byte], offset: Double, shape: Array[Int]): Array[Double] = {
    val numElems = shape.product
    val array = byteArray.slice(0, numElems)
    val SourceArray = byteArray.map(floatByte => (floatByte & 0xff).asInstanceOf[Float].toDouble + offset)
    SourceArray
  }

}
