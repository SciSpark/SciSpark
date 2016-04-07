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

import scala.collection.mutable

class MCCNode(var frameNum :Int, var cloudElemNum : Double) extends Serializable {

  override def toString = s"($frameNum,$cloudElemNum)"

  override def equals(that: Any): Boolean = that match {
    case that: MCCNode => that.frameNum == this.frameNum && that.cloudElemNum == this.cloudElemNum
    case _ => false
  }

  //  object NodeType extends Enumeration {
  //    type NodeType = Value
  //    val source = Value("Source")
  //    val dest = Value("Destination")
  //  }

  var inEdges : mutable.HashSet[MCCEdge] = new mutable.HashSet[MCCEdge]
  var outEdges : mutable.HashSet[MCCEdge] = new mutable.HashSet[MCCEdge]

  def connectTo(destNode: MCCNode, weight: Double): MCCEdge = {
    val edge = new MCCEdge(this, destNode, weight)
    addOutgoingEdge(edge)
    return edge
  }

  def connectFrom(srcNode: MCCNode, weight: Double): MCCEdge = {
    val edge = new MCCEdge(this, srcNode, weight)
    addIncomingEdge(edge)
    return edge
  }

  def addIncomingEdge(edge: MCCEdge) = {
    inEdges += edge
  }

  def addOutgoingEdge(edge: MCCEdge) = {
    outEdges += edge
  }

  def getFrameNum : Int = {
    frameNum
  }

  def getCloudElemNum : Double = {
    cloudElemNum
  }

  def setFrameNum (f : Int) = {
    frameNum = f
  }

  def setCloudElemNum (c : Float) = {
    cloudElemNum = c
  }
}
