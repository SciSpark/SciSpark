package org.dia.core

import org.apache.spark.{ Partition, TaskContext }
import scala.reflect.ClassTag

/**
 * The map partition used by SRDD to perform the Map operation.
 */
class SMapPartitionsRDD[U: ClassTag, T: ClassTag](prev: SRDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false) extends SRDD[U](prev) {

  //TODO :: avoiding partitioner for now
  override val partitioner = if (preservesPartitioning) firstParent[T].partitioner else None

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split.index, firstParent[T].iterator(split, context))

}