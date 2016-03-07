package org.dia.algorithms.mcc


class MCCEdge(val srcNode : MCCNode, val destNode: MCCNode, val weigth : Float) extends Serializable {

  override def toString = s"($srcNode, $destNode, $weigth)"

  override def equals(that: Any): Boolean = that match {
    case that: MCCEdge => that.srcNode == this.srcNode && that.destNode == this.destNode
  }
  def this(srcNode : MCCNode, destNode: MCCNode) {
    this(srcNode, destNode, 0f)
  }
}