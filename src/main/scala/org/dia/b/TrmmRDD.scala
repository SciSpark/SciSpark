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
package org.dia.b

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.dia.Constants._

import scala.language.implicitConversions
import scala.reflect.ClassTag

/**
 *
 */
object TrmmRDD {
  implicit class RichRDD[T: ClassTag](rdd: RDD[T]) {

    def countEachElement = {
      rdd
        .map(element => (element, 1))
        .reduceByKey((value1, value2) => value1 + value2)
    }

    def countWhere(f: T => Boolean): Long = {
      rdd.filter(f).count()
    }
  }

}

