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
package org.dia.algorithms.mcs

import scala.collection.mutable

class MCSEdge(var srcNode : MCSNode, var destNode: MCSNode, var weight : Double) extends Serializable {

  var areaOverlap: Int = 0
  var metadata: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()

  def this(srcNode : MCSNode, destNode: MCSNode) {
    this(srcNode, destNode, 0.0)
  }

  def setSourceNode(node: MCSNode): Unit = {
    this.srcNode = node
  }

  def incrementAreaOverlap(): Unit = {
    this.areaOverlap += 1
  }

  override def equals(that: Any): Boolean = that match {
    case that: MCSEdge => that.srcNode == this.srcNode && that.destNode == this.destNode
  }

  def updateWeight(_weight: Double): Unit = {
    this.weight = _weight
  }

  def updateMetadata(key: String, value: String): MCSEdge = {
    this.metadata.update(key, value)
    this
  }

  def hashKey(): String = {
    s"${this.srcNode.hashKey()},${this.destNode.hashKey()  }"
  }
  override def toString : String = {
    s"((${this.srcNode.frameNum}:${this.srcNode.cloudElemNum})," +
      s" (${this.destNode.frameNum}:${this.destNode.cloudElemNum}))"
  }

  override def hashCode(): Int = super.hashCode()

}
