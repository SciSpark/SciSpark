package org.dia.algorithms.mcc


class MCCEdge(var srcNode : MCCNode, var destNode: MCCNode, var weight : Float) extends Serializable {

  override def toString = s"($srcNode, $destNode, $weight)"

  override def equals(that: Any): Boolean = that match {
    case that: MCCEdge => that.srcNode == this.srcNode && that.destNode == this.destNode
  }
  def this(srcNode : MCCNode, destNode: MCCNode) {
    this(srcNode, destNode, 0f)
  }

  def setSourceNode(node: MCCNode) = {
    this.srcNode = node
  }
}