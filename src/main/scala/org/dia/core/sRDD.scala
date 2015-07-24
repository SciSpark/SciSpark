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
import org.dia.tensors.TensorFactory

import scala.reflect.ClassTag

/**
 * Scientific RDD class
 * @param sc  SparkContext
 * @param datasets  list of URIs to be used
 * @param varName   Variable name to be used
 *                  //TODO the default constructor shouldn't take the varName anymore
 * @param loadFunc  Loading function
 * @param partitionFunc Partitioning function
 * @tparam T  Type to be used. It should be sciTensor
 */
class sRDD[T: ClassTag](val sc : SparkContext, val deps : Seq[Dependency[_]]) extends RDD[T](sc, deps) with Logging {
  var datasets : List[String] = null
  var varName : String = null
  var loadFunc : (String, String) => (Array[Double], Array[Int]) = null
  var partitionFunc : List[String] =>List[List[String]] = null
  val arrLib = sc.getLocalProperty(org.dia.Constants.ARRAY_LIB)

def this (sc: SparkContext, data: List[String], name: String, loader: (String, String) => (Array[Double], Array[Int]), partitionerer: List[String] => List[List[String]]) {
  this(sc, Nil)
  datasets = data
  varName = name
  loadFunc = loader
  partitionFunc = partitionerer
}

  def this(@transient oneParent : sRDD[_]) = {
    this(oneParent.context , List(new OneToOneDependency(oneParent)))
  }

  override def context : SparkContext = sc

  /**
   * Return an array that contains all of the elements in this RDD.
   */
  override def collect(): Array[T] = {
    val results = sc.runJob(this, (iter: Iterator[T]) => iter.toArray)
    Array.concat(results: _*)
  }
//   TODO this needs to handled
//  var sRddDeps: Seq[Dependency[_]] = null

//  def this(@transient _sc: SparkContext, @transient deps: Seq[Dependency[_]]) = {
//    this(_sc, null, "", null, null)
////    sRddDeps = deps
//  }
//
//  /** Construct an RDD with just a one-to-one dependency on one parent */
//  def this(@transient oneParent: RDD[_]) = {
//    this(oneParent.context , List(new OneToOneDependency(oneParent)))
//  }


  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   * Computes the iterator needed according to the array lib needed.
   */
  def compute(split: Partition, context: TaskContext): Iterator[T] = {
    // call the loader/constructor
    getIterator(split.asInstanceOf[sRDDPartition[T]])
  }

  def getIterator(theSplit: sRDDPartition[T]): Iterator[T] = {
    val iterator = new Iterator[T] {
      var counter = 0

      //
      override def hasNext: Boolean = {
        counter < theSplit.uriList.length
      }

      override def next(): T = {

        val urlValue = theSplit.uriList(counter)
        val loader = () => {loadFunc(urlValue, varName)}
        val tensor = TensorFactory.getTensors(arrLib, loader)

        counter += 1
        val sciArray = new sciTensor(varName, tensor)
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
    // will create a list of lists of empty sTensors
    val listOfLists = partitionFunc(datasets)
    val array = new Array[Partition](listOfLists.size)

    for (list <- listOfLists) {
      array(pos) = new sRDDPartition(pos, list)
      pos += 1
    }
    array
  }

//  /**
//   * Return a new RDD by applying a function to all elements of this RDD.
//   */
//  def map[U: ClassTag](f: T => U): sRDD[U] = {
//    val cleanF = sc.clean(f)
//    new sRDDPartition[U, T](this, (context, pid, iter) => iter.map(cleanF))
//  }
  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   */
  override def map[U: ClassTag](f: T => U): sRDD[U] = {
    new sMapPartitionsRDD[U, T](this, (sc, pid, iter) => iter.map(f))
  }


}