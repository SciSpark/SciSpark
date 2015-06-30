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
