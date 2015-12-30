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
package org.dia.algorithms.pdfclustering

import org.dia.loaders.NetCDFReader
import org.dia.utils.NetCDFUtils
import ucar.nc2.dataset.NetcdfDataset
import org.dia.core.SciSparkContext

object MainCompute {

  def main(args: Array[String]): Unit = {

    val partCount = 2

    val netcdfDir = "resources/merra"
    
    /** TSURF(time, lat, lon) */
    val variables = List("TSURF")

    val masterURL = "local[2]"

    val sc = new SciSparkContext(masterURL, "PDF clustering")

    /**
     *  Each SciTensor of this RDD here is of the form T(time,lat,lon)
     *  where T = surface temperature.
     */
    val rdd = sc.NetcdfDFSFile(netcdfDir, variables, partCount)

  }

}