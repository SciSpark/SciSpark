/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dia.core

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.dia.tensors.{AbstractTensor, TensorFactory}

import scala.collection.mutable

//import scala.collection.immutable.HashMap
//import scala.collection.parallel.mutable
import scala.reflect.ClassTag

/**
 * Scientific RDD. There are few differences between an sRDD and a normal RDD.
 * In the future we will extend repartitioning by time and by space.
 *
 * Currently the sRDD differs from a normal RDD in the following ways.
 * 1) Supports custom matrix loader functions with the following signature :
 * (String, String) => (Array[Double], Array[Int]) which is the 1-D array and dimensional shape
 * 2) Supports custom partitioning functions with the following signature :
 * List[String] => List[List[String]]
 *
 * Note that passing closures has it's drawbacks if the scope of a closure environment is not properly checked.
 * The internal API of the sRDD (spefically the constructors) may change in the future.
 *
 */
class sRDD[T: ClassTag](@transient var sc: SparkContext, @transient var deps: Seq[Dependency[_]]) extends RDD[T](sc, deps) with Logging {
  val arrLib = sc.getLocalProperty(org.dia.Constants.ARRAY_LIB)
  var datasets: List[String] = null
  var varName: Seq[String] = Nil
  var loadFunc: (String, String) => (Array[Double], Array[Int]) = null
  var partitionFunc: List[String] => List[List[String]] = null

  def this(@transient sc: SparkContext,
           data: List[String],
           name: List[String],
           loader: (String, String) => (Array[Double], Array[Int]),
           partitioner: List[String] => List[List[String]]) {

    this(sc, Nil)
    datasets = data
    varName = name
    loadFunc = loader
    partitionFunc = partitioner
  }

  //  def this(@transient sc: SciSparkContext,
  //           data: List[String],
  //           name: List[String],
  //           loader: (String, String) => (Array[Double], Array[Int]),
  //           partitioner: List[String] => List[List[String]]) {
  //
  //    this(sc.sparkContext, data, name, loader, partitioner)
  //  }

  def this(@transient oneParent: sRDD[_]) = {
    this(oneParent.context, List(new OneToOneDependency(oneParent)))
  }

  override def context: SparkContext = sc

  /**
   * Return an array that contains all of the elements in this RDD.
   */
  override def collect(): Array[T] = {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
  }

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   * Computes the iterator needed according to the array lib needed.
   */
  def compute(split: Partition, context: TaskContext): Iterator[T] = {
    getIterator(split.asInstanceOf[sRDDPartition[T]])
  }

  /**
   * Given an sRDDPartition, the iterator traverses the list of URL's
   * assigned to the partition. Calls to next() will call the loader function
   * and construct a SciTensor for each URL in the list.
   */
  def getIterator(theSplit: sRDDPartition[T]): Iterator[T] = {
    val iterator = new Iterator[T] {
      var counter = 0

      override def hasNext: Boolean = {
        counter < theSplit.uriList.length
      }

      override def next(): T = {
        val urlValue = theSplit.uriList(counter)
        val tensorMap = varName.map(avar => {
          val loader = () => {
            loadFunc(urlValue, avar)
          }
          (avar, TensorFactory.getTensors(arrLib, loader))
        }).toMap
        val hash = new mutable.HashMap[String, AbstractTensor]
        tensorMap.map(p => hash += p)
        counter += 1

        val sciArray = new SciTensor(hash)
        sciArray.insertDictionary(("SOURCE", urlValue))
        sciArray.asInstanceOf[T]
      }
    }
    iterator
  }

  /**
   * Returns the set of partitions in this RDD. Each partition represents a single URLs.
   * The default setting is a grouping of 1 url.
   *
   * @return
   */
  override def getPartitions: Array[Partition] = {
    var pos = 0
    val listOfLists = partitionFunc(datasets)
    val array = new Array[Partition](listOfLists.size)

    for (list <- listOfLists) {
      array(pos) = new sRDDPartition(pos, list)
      pos += 1
    }
    array
  }

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  override def map[U: ClassTag](f: T => U): sRDD[U] = {
    new sMapPartitionsRDD[U, T](this, (sc, pid, iter) => iter.map(f))
  }

  override def flatMap[U: ClassTag](f: T => TraversableOnce[U]): sRDD[U] = {
    new sMapPartitionsRDD[U, T](this, (context, pid, iter) => iter.flatMap(f))
  }

  override def filter(f: T => Boolean): sRDD[T] = {
    new sMapPartitionsRDD[T, T](
      this,
      (context, pid, iter) => iter.filter(f),
      preservesPartitioning = true)
  }

}