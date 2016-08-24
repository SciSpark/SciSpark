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
package org.dia.algorithms.mcc

class MCCEdge(var srcNode : MCCNode, var destNode: MCCNode, var weight : Double) extends Serializable {

  var areaOverlap: Int = 0

  def incrementAreaOverlap(): Unit = {
    areaOverlap += 1
  }

  def updateWeight(_weight: Double): Unit = {
    weight = _weight
  }

  def this(srcNode : MCCNode, destNode: MCCNode) {
    this(srcNode, destNode, 0.0)
  }
  override def toString : String = {
    s"((${srcNode.frameNum},${srcNode.cloudElemNum}) , (${destNode.frameNum},${destNode.cloudElemNum}))"
  }

  override def equals(that: Any): Boolean = that match {
    case that: MCCEdge => that.srcNode == this.srcNode && that.destNode == this.destNode
  }

  override def hashCode(): Int = super.hashCode()

  def setSourceNode(node: MCCNode): Unit = {
    this.srcNode = node
  }
}
