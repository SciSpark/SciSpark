package org.dia.core

object SparkTestConstants {
  val sc = new SciSparkContext("local[4]", "test")
  val datasetPath = "src/main/resources/TestLinks"
  val datasetVariable = "precipitation"
}
