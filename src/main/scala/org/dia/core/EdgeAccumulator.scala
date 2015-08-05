package org.dia.core

import org.apache.spark.AccumulatorParam


object EdgeAccumulator extends AccumulatorParam[List[(Long, Long)]]{
  def zero(initialValue: List[(Long, Long)]): List[(Long, Long)] = {
    Nil
  }

  def addInPlace(v1: List[(Long, Long)], v2: List[(Long, Long)]): List[(Long,Long)] = {
    v1 ++ v2
  }
}
