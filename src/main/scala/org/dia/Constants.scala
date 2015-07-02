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
 * Constants
 */
object Constants {
  // mapping between data set and variables
  // TODO find a better way to retrieve the variables per dataSet
  val DATASET_VARS = collection.immutable.HashMap("TRMM_L3" -> "data", "ncml" -> "TotCldLiqH2O_A")
  // Rows dimension name
  val TRMM_ROWS_DIM = "rows"
  // Cols dimension name
  val TRMM_COLS_DIM = "cols"
  // TRMM default row dimension size
  val DEFAULT_TRMM_ROW_SIZE = 400
  // TRMM default col dimension size
  val DEFAULT_TRMM_COL_SIZE = 1440
}
