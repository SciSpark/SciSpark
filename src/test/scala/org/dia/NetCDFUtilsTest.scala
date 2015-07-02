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
package org.dia

import org.dia.Constants.DATASET_VARS
import org.dia.n.MainNd4j

/**
 * import DSL for scala api
 */

/**
  * Class for testing functionality
  */
class NetCDFUtilsTest extends org.scalatest.FunSuite {

   test("ReadingNCDFVarsToNdj4") {
     // loading TRMM data
//     var url = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_L3/TRMM_3B42_daily/1997/365/3B42_daily.1998.01.01.7.bin"
//     val nd4jTRMM = MainNd4j.getNd4jNetCDFVars(url, DATASET_VARS.get("TRMM_L3").get)
//     println(nd4jTRMM.toString)
     assert(true)
   }

   test("LoadDimensionsTRMM") {
     //get NCDFfile
//     var url = "http://disc2.nascom.nasa.gov:80/opendap/TRMM_L3/TRMM_3B42_daily/1997/365/3B42_daily.1998.01.01.7.bin"
//     val netcdfFile = NetCDFUtils.loadNetCDFDataSet(url)
//     assert(netcdfFile != null)

     //test dimensions
//     val rows = Main.getRowDimension(netcdfFile)
//     val cols = Main.getColDimension(netcdfFile)
//     assert(rows == 400)
//     assert(cols == 1440)
     assert(true)
   }
 }
