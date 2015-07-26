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
package org.dia.partitioners

import org.dia.Constants

import scala.language.implicitConversions

/**
 * Functions needed to map keys to values
 */
object sTrmmPartitioner {

  /**
   * Groups strings by the string passed along
   */
  def mapOneUrlToManyTensorTRMM(urls: List[String], groupedBy: String): List[List[String]] = {
    var mappedUrls: List[List[String]] = Nil
    var pos = 0
    groupedBy match {

      case Constants.GROUP_BY_DAY => pos = 8;
      case Constants.GROUP_BY_YEAR => pos = 4;
    }
    val groups = urls.groupBy(_.replace(Constants.TRMM_HOURLY_URL, "").substring(0, pos))
    groups.map(entry => mappedUrls = mappedUrls ::: List(entry._2))
    mappedUrls
  }

  def mapOneYearToManyTensorTRMM(urls: List[String]): List[List[String]] = {
    var mappedUrls: List[List[String]] = Nil
    //We know how the url is constructed so this shouldn't be a problem
    var pos = 4
    val groups = urls.groupBy(_.replace(Constants.TRMM_HOURLY_URL, "").substring(0, pos))
    groups.map(entry => mappedUrls = mappedUrls ::: List(entry._2))
    mappedUrls
  }

  def mapOneDayToManyTensorTRMM(urls: List[String]): List[List[String]] = {
    var mappedUrls: List[List[String]] = Nil
    //We know how the url is constructed so this shouldn't be a problem
    var pos = 8
    val groups = urls.groupBy(_.replace(Constants.TRMM_HOURLY_URL, "").substring(0, pos))
    groups.map(entry => mappedUrls = mappedUrls ::: List(entry._2))
    mappedUrls
  }
}

