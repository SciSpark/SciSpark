package org.dia.core

import org.apache.spark.SparkContext
import org.dia.NetCDFUtils
import org.nd4j.linalg.api.ndarray.INDArray

import scala.io.Source
/**
 * SciSpark contexts extends the existing SparkContext function.
 * However there are many private functions within SparkContext
 * that are useful for catching unwanted calls. Such as
 * executing one of the functions after the SparkContext has been stopped.
 *
 * TODO :: Should we extend SparkContext or modify a copy of SparkContext
 * Created by rahulsp on 7/8/15.
 */
class SciSparkContext(master : String, appName : String) extends SparkContext(master, appName) {

  /**
   * Constructs an sRDD from a file of openDap URL's pointing to NetCDF datasets.
   *
   * TODO :: Support for reading more than one variable
   * TODO :: Properly integrate minimum partitioning
   *
   * @param path Path to a file containing a list of OpenDap URLs
   * @param varName the variable name to search for
   * @param minPartitions the minimum number of partitions
   * @return
   */
    def OpenDapURLFile(path: String,
                       varName : String,
                       minPartitions: Int = defaultMinPartitions) : sRDD[INDArray] = {

      val datasetUrls = Source.fromFile(path).mkString.split("\n").toList
      new sRDD[INDArray](this, datasetUrls, varName)
    }



}
