package org.dia.algorithms.mcc

import scala.collection.mutable

class MCCNode(var frameNum :Int, var cloudElemNum : Float) extends Serializable {

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
  var isBorder = false

  def connectTo(destNode: MCCNode, weight: Float): MCCEdge = {
    val edge = new MCCEdge(this, destNode, weight)
    addOutgoingEdge(edge)
    return edge
  }

  def connectFrom(srcNode: MCCNode, weight: Float): MCCEdge = {
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

  def getCloudElemNum : Float = {
    cloudElemNum
  }

  def setFrameNum (f : Int) = {
    frameNum = f
  }

  def setCloudElemNum (c : Float) = {
    cloudElemNum = c
  }
}
