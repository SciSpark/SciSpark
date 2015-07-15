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

import org.apache.spark.Partition

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
 * Created by marroquin on 7/13/15.
 */
class sRDDPartition[T: ClassTag] (
                              idx: Int,
                              val dataset: List[T]
                              ) extends Partition {
  /**
   * Partition index
   */
  override def index: Int = idx

  /**
   * To string method
   * @return String
   */
  override def toString() = {
    val sb = new StringBuilder
    sb.append("{idx:").append(idx).append(",")
    sb.append("urls:").append(dataset).append("}")
    sb.toString
  }
}
