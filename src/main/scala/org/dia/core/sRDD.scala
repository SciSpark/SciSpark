package org.dia.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.dia.n.Nd4jFuncs

import scala.reflect.ClassTag

/**
 * Created by rahulsp on 7/8/15.
 */

private class sRDDPartition(
                             idx: Int,
                             val datasetURL: List[String]
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
    sb.append("{idx:").append(idx).append(", ");
    sb.append("readings:").append("}")
    sb.toString
  }
}


class sRDD[T: ClassTag](sc: SparkContext,
                        datasetURLs: List[String],
                        varName: String)
  extends RDD[T](sc, Nil) with Logging {

  /**
   *
   * Returns the set of partitions in this RDD. Each partition is a grouped set of URLs.
   * The default setting is a grouping of 1 url.
   *
   * TODO :: Explore custom partitioners
   * @return
   */
  override def getPartitions: Array[Partition] = {
    var pos = 0
    val array = new Array[Partition](datasetURLs.length)
    for (urlPartition <- datasetURLs.grouped(1)) {
      array(pos) = new sRDDPartition(pos, urlPartition)
      pos += 1
    }
    array
  }

  /**
   * Computes the sRDD elements by pulling them from the OpenDap URLs
   * TODO :: Decouple sRDD compute from the source url - instead use NetCDFDataset
   * @param theSplit
   * @param context
   * @return
   */
  override def compute(theSplit: Partition, context: TaskContext): Iterator[T] = {
    val split = theSplit.asInstanceOf[sRDDPartition]
    val iterator = new Iterator[T] {
      var counter = 0

      override def hasNext: Boolean = {
        counter < split.datasetURL.length
      }

      override def next: T = {
        val tensor = Nd4jFuncs.getNetCDFNDVars(split.datasetURL(counter), varName)
        counter += 1
        tensor.asInstanceOf[T]
      }
    }

    iterator
  }
}
