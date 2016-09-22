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
package org.dia.core

import java.io.File
import java.net.URI

import scala.language.implicitConversions

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import org.apache.spark.rdd.RDD

import org.dia.tensors.AbstractTensor

/**
 * Functions on top of the SRDD: an RDD of SciDatasets
 * To use the functions in SRDDFunctions import it like so :
 *
 * import org.dia.core.SRDDFunctions._
 *
 * You can call the functions on all RDD's of type RDD[SciTensor]
 *
 * @param self the RDD to call functions on
 */
class SRDDFunctions(self: RDD[SciDataset]) extends Serializable {

  /**
   * Writes the RDD of SciDatasets under a directory in hdfs.
   * This is a quick fix function that writes to the local filesystem
   * under tmp and the copies it to hdfs.
   * TODO :: Write netcdfFile directly to hdfs rather to local fs and then copying over.
   *
   * @param directoryPath The directory to write to. The hdfs path format is HDFS://HOSTNAME:<port no.>
   * @param stagingPath The area to stage files on the local filesystem. Default is /tmp/.
   */
  def writeSRDD(directoryPath : String, stagingPath : String = "/tmp/"): Unit = {
    self.foreach(p => {
      p.writeToNetCDF(p.datasetName, stagingPath)
      val conf = new Configuration()
      val fs = FileSystem.get(new URI(directoryPath), conf)
      FileUtil.copy(new File(stagingPath + p.datasetName), fs, new Path(directoryPath), true, conf)
    })
  }

  /**
   * Splits Variables in SciDatasets into tiled Variables.
   *
   * The resulting tile variables are mapped to two keys:
   * - a list of ranges specifying the location of the tile
   * in the original array
   * - an index specifying the tile's location on a new axis.
   *
   * @param varName The name of the variable to use
   * @param keyFunc the function used to extract a new index
   * @param tileShape the shape of tiles the variable array
   *                       wil be split into
   * @return
   */
  def splitTiles(
      varName: String,
      keyFunc: SciDataset => Int,
      tileShape: Int*): RDD[(List[(Int, Int)], (Int, Variable))] = {
    self.flatMap(sciD => {
      val shape = sciD(varName).shape()
      val tileShapeSeq = if (tileShape.isEmpty) shape.toSeq else tileShape

      /**
       * Obtain a sliding range across all dimensions
       * For example : An array of dimension 4, 4, 4 and a tile shape of 2 x 2 x 2
       * will result in the following structure
       * List(
       *  List((0, 2), (2, 4))
       *  List((0, 2), (2, 4))
       *  List((0, 2), (2, 4))
       * )
       */
      val tileRangesAlongDimensions = tileShapeSeq.zipWithIndex.map({
        case (subLen, index) => (0 to shape(index) by subLen).sliding(2).map(seq => (seq(0), seq(1)))
      }).map(t => t.map(z => List(z)).toList)

      /**
       * Take the cross product of ranges across dimension
       * resulting in range slices for all tiles of the given shape.
       *
       * Following the example from above :
       * List(
       *  List((0, 2), (0, 2), (0, 2))
       *  List((0, 2), (0, 2), (2, 4))
       *  List((0, 2), (2, 4), (0, 2))
       *  List((0, 2), (2, 4), (2, 4))
       *  List((2, 4), (0, 2), (0, 2))
       *  List((2, 4), (0, 2), (2, 4))
       *  List((2, 4), (2, 4), (0, 2))
       *  List((2, 4), (2, 4), (2, 4))
       * )
       */
      val ranges = tileRangesAlongDimensions.reduce((ls1, ls2) => for (l1 <- ls1; l2 <- ls2) yield l1 ++ l2)

      /**
       * Apply the ranges on the variable to split it into tiles.
       */
      ranges.map(range => (range, (keyFunc(sciD), sciD(varName)(range: _*))))
    })
  }

  /**
   * Stacks a collection of variables along a new axis.
   *
   * Each element in the input RDD consists of :
   *  1. List of ranges that serve as the location key
   *  2. A tuple consisting of:
   *      a. The index of the variable tile on the new axis
   *      b. The variable tile
   * @param sRDD the input RDD
   * @param dimName the name of the new dimension default : time
   * @return
   */
  def stackTiles(sRDD: RDD[(List[(Int, Int)], (Int, Variable))], dimName : String = "time"): RDD[Variable] = {
    /**
     * ReduceByKey aggregates tiles with the same original position
     */
    sRDD.map({ case (rangeKey, p) => (rangeKey, List(p))})
      .reduceByKey(_ ++ _)
      .map({
        case (rangeKey, keyedVariables) =>

          /**
           * Use a sample variable to obtain the dimensions of
           */
          val (_, sampleVar) = keyedVariables.head
          val newDims = (dimName, keyedVariables.length)::sampleVar.dims

          /**
           * Sort the tiles according to their index on the new axis.
           * Stack tiles along this axis to create a tensor object.
           */
          val sortedTensors = keyedVariables
            .sortBy({case (key, _) => key})
            .map({case (_, variable) => variable()})
          val head::tail = sortedTensors
          val stackedTensor = head.stack(tail: _*)

          new Variable(sampleVar.name, stackedTensor, newDims)
      })
  }

  /**
   * Repartitions a variable stored in each SciDataset.
   * Initially the variable is split across a given axis for example time.
   * Each SciDataset then corresponds to a specific index on the time axis.
   *
   * repartitionBySpace splits the chosen variable into tiles, and aggregates
   * them along the time (or user defined) axis.
   * @param varName the name of the variable
   * @param keyFunc the function used to obtain index on the new axis
   * @param tileShape the shape of tiles to each variable into.
   * @return
   */
  def repartitionBySpace(
      varName: String,
      keyFunc: SciDataset => Int,
      tileShape: Int*): RDD[Variable] = {
    val rangeKeyedRDD = splitTiles(varName, keyFunc, tileShape: _*)
    stackTiles(rangeKeyedRDD)
  }

  /**
   * For each array N* where N is the frame number and N* is the array
   * output the following pairs (N, N*), (N + 1, N*).
   *
   * After flat-mapping the pairs and applying additional pre-processing
   * we have pairs (X, Y) where X is a label and Y a tensor.
   *
   * After reducing by key and reordering pairs we obtain pairs
   * (N*, (N+1)*) which achieves the consecutive pairwise grouping
   * of frames.
   *
   * Precondition : Each SciTensor has a FRAME key recorded in its metadata table
   *
   * @param sRDD the input RDD of SciTensors
   * @return
   */
  def pairConsecutiveFrames(frame: String): RDD[(SciDataset, SciDataset)] = {
    self.sortBy(p => p.attr(frame).toInt)
      .zipWithIndex()
      .flatMap({ case (sciD, indx) => List((indx, List(sciD)), (indx + 1, List(sciD))) })
      .reduceByKey(_ ++ _)
      .filter({ case (_, sciDs) => sciDs.size == 2 })
      .map({ case (_, sciDs) => sciDs.sortBy(p => p.attr(frame).toInt) })
      .map(sciDs => (sciDs(0), sciDs(1)))
  }
}

object SRDDFunctions {

  /** Implicit conversion from an RDD of SciDatasets to
   *  RDDFunctions of SciDatasets
   */
  implicit def fromRDD(rdd: RDD[SciDataset]): SRDDFunctions = new SRDDFunctions(rdd)
}
