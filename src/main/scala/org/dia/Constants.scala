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

/**
 * Constants relevant across the SciSpark source tree
 */
object Constants extends Serializable {
  /**
   * mapping between compData set and variables
   *  @todo find a better way to retrieve the variables per dataSet
   */

  /** potential names for the different axis of the grid that is present in NetCDF files */
  val X_AXIS_NAMES = Array("x", "rlat", "rlats", "lat", "lats", "rows", "nlat", "latitude", "latitudes")
  val Y_AXIS_NAMES = Array("y", "rlon", "rlons", "lon", "lons", "cols", "nlon", "longitude", "longitudes")
  val TIME_NAMES = Array("time", "times", "date", "dates", "julian")

  /** TRMM default row/col dimension size */
  val DEFAULT_TRMM_ROW_SIZE = 400
  val DEFAULT_TRMM_COL_SIZE = 1440

  /** TRMMHourly tensors URL */
  val TRMM_HOURLY_URL = "http://disc2.nascom.nasa.gov/opendap/TRMM_3Hourly_3B42/"
  val TRMM_HOURLY_DATA_PREFFIX = "3B42"
  val TRMM_HOURLY_DATA_SUFFIX = ".7.HDF.Z"

  val ARRAY_LIB = "array-lib"
  val BREEZE_LIB = "breeze"
  val ND4J_LIB = "nd4j"

  val GROUP_BY_YEAR = "group_by_year"
  val GROUP_BY_DAY = "group_by_day"

}
