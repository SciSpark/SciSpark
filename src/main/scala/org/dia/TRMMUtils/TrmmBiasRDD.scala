package org.dia.TRMMUtils

import java.io.{IOException, ObjectOutputStream}

import breeze.linalg.DenseMatrix
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.dia.b.TrmmHourlyRDD

import scala.reflect.ClassTag

class TrmmBiasPartition(
                         idx: Int,
                         @transient rdd1: RDD[_],
                         @transient rdd2: RDD[_],
                         s1Index: Int,
                         s2Index: Int
                         ) extends Partition {
  var s1 = rdd1.partitions(s1Index)
  var s2 = rdd2.partitions(s2Index)
  override val index: Int = idx

  private def writeObject(oos: ObjectOutputStream): Unit = {
    try {
      // Update the reference to parent split at the time of task serialization
      s1 = rdd1.partitions(s1Index)
      s2 = rdd2.partitions(s2Index)
      oos.defaultWriteObject()
    } catch {
      case e: IOException => println("Error while processing TrmmBiasPartition")
    }
  }
}

/**
  */
class TrmmBiasRDD[T: ClassTag](
                                sc: SparkContext,
                                rdd1: TrmmHourlyRDD[T],
                                rdd2: TrmmHourlyRDD[T])
  extends RDD[T](sc, Nil) with Logging {

  // We could use any of the rdd's partitions
  val numPartitionsInRdd2 = rdd2.partitions.length

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    var splitt = split.asInstanceOf[TrmmBiasPartition]
    val rddIt1 = rdd1.iterator(splitt.s1, context)
    val rddIt2 = rdd2.iterator(splitt.s2, context)

    val iter = new Iterator[T] {

      override def hasNext: Boolean = rddIt1.hasNext && rddIt2.hasNext

      override def next(): T = {
        val xx = rddIt2.next()
        val yy = rddIt1.next()
        val label = xx.asInstanceOf[(String, DenseMatrix[Double])] + ":" + yy.asInstanceOf[(String, DenseMatrix[Double])]._1
        var resVal = xx.asInstanceOf[(String, DenseMatrix[Double])]._2
        if (resVal == null)
          resVal = yy.asInstanceOf[(String, DenseMatrix[Double])]._2
        else if (yy.asInstanceOf[(String, DenseMatrix[Double])]._2 != null)
          resVal = xx.asInstanceOf[(String, DenseMatrix[Double])]._2 - yy.asInstanceOf[(String, DenseMatrix[Double])]._2
        (label, resVal).asInstanceOf[T]
      }
    }
    iter
  }

  override protected def getPartitions: Array[Partition] = {
    val array = new Array[Partition](rdd2.partitions.length)
    for (s1 <- rdd1.partitions; s2 <- rdd2.partitions) {
      val idx = s1.index * numPartitionsInRdd2 + s2.index
      array(idx) = new TrmmBiasPartition(idx, rdd1, rdd2, s1.index, s2.index)
    }
    array
  }

}
