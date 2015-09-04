package org.dia.TestEnvironment

import org.dia.core.SciSparkContext

object SparkTestConstants {
  val sc = new SciSparkContext("local[4]", "test")
  val datasetPath = "src/test/resources/TestLinks"
  val datasetVariable = "data"
}
