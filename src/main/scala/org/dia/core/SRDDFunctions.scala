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
   */
  def writeSRDD(directoryPath : String): Unit = {
    self.foreach(p => {
      p.writeToNetCDF(p.datasetName, "/tmp/")
      val conf = new Configuration()
      val fs = FileSystem.get(new URI(directoryPath), conf)
      FileUtil.copy(new File("/tmp/" + p.datasetName), fs, new Path(directoryPath), true, conf)
    })
  }

  def splitBySubset(varName : String,
                    keyFunc : SciDataset => Int,
                    subsetShape : Int*): RDD[(List[(Int, Int)], Int, AbstractTensor)] = {
    self.flatMap(sciD => {
      val shape = sciD(varName).shape()
      val ks = subsetShape.zipWithIndex.map({
        case(subLen, index) => (0 to shape(index) by subLen).sliding(2).map(seq => (seq(0), seq(1)))
      }).map(t => t.map(z => List(z)).toList)
      val ranges = ks.reduce((ls1, ls2) => for (l1 <- ls1; l2 <- ls2) yield l1 ++ l2)
      ranges.map(range => (range, keyFunc(sciD), sciD(varName)()(range: _*)))
    })
  }
}

object SRDDFunctions {

  /** Implicit conversion from an RDD of SciDatasets to
   *  RDDFunctions of SciDatasets
   */
  implicit def fromRDD(rdd: RDD[SciDataset]): SRDDFunctions = new SRDDFunctions(rdd)
}
