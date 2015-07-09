package org.dia.core

import org.apache.spark.SparkContext

/**
 * Created by rahulsp on 7/9/15.
 */
object SparkTestConstants {
  val sc = new SciSparkContext("local[4]", "test")
}
