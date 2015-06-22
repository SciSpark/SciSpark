package org.dia

import org.apache.spark.{SparkConf, SparkContext}
import org.jblas.DoubleMatrix
import ucar.ma2
import ucar.nc2.dataset.NetcdfDataset
/**
 * Created by rahulsp on 6/17/15.
 */
object Main {

  /** 
   * Variable names
    */
  val TotCldLiqH2O = "TotCldLiqH2O_A"
  val data = "data"

  /**
   * The url is the base url where the netcdf file is located.
   * 1) Fetch the variable array from via the NetCDF api
   * 2) Download and convert the netcdf array to 1D array of doubles
   * 3) Reformat the array as a jblas Double Matrix, and reshape it with the original coordinates
   * 
   * TODO :: How to obtain the array dimensions from the netcdf api, 
   *         instead of hardcoding for reshape function
   * @param url
   * @param variable
   * @return
   */
  def getNetCDFVars (url : String, variable : String) : DoubleMatrix = {
    val netcdfFile = NetcdfDataset.openDataset(url);
    val SearchVariable: ma2.Array = netcdfFile.findVariable(variable).read()

    val coordinateArray = SearchVariable.copyTo1DJavaArray().asInstanceOf[Array[Float]].map(p => p.toDouble)
    val matrix = new DoubleMatrix(coordinateArray).reshape(14, 360)
    matrix
  }

  /**
   * 
   * @param largeArray
   * @param blockSize
   * @return
   */
  def jblasreduceResolution(largeArray : DoubleMatrix, blockSize : Int) : DoubleMatrix =  {
    val numRows = largeArray.rows
    val numCols = largeArray.columns

    val reducedSize = numRows * numCols / (blockSize * blockSize)

    val reducedMatrix = DoubleMatrix.zeros(numRows / blockSize, numCols / blockSize)
    for(row <- 0 to reducedMatrix.rows - 1){
      for(col <- 0 to reducedMatrix.columns - 1){
        val block = largeArray.getRange(row * blockSize, ((row + 1) * blockSize) , col * blockSize,  ((col + 1) * blockSize))
        val average = block.mean
        reducedMatrix.put(row, col, average)
      }
    }

    reducedMatrix
  }

  def breezereduceResolution(largeArray : DoubleMatrix, blockSize : Int) : DoubleMatrix = {

  }

  def main(args : Array[String]) : Unit = {
    OpenDapURLGenerator.run()
    val conf = new SparkConf().setAppName("L").setMaster("local[4]")
    val sparkContext = new SparkContext(conf)
    val urlRDD = sparkContext.textFile("Links").repartition(4)

    /**
     * Uncomment this line in order to test on a normal scala array
     * val urlRDD = Source.fromFile("Links").mkString.split("\n")
     */

    val HighResolutionArray = urlRDD.map(url => getNetCDFVars(url, TotCldLiqH2O))

    val LowResolutionArray = HighResolutionArray.map(largeArray => jblasreduceResolution(largeArray, 20))
    println(LowResolutionArray.count)

    LowResolutionArray.collect.map(p => {
      println("[")
      for(i <- 0 to p.rows - 1){
        for(j <- 0 to p.columns - 1){
          print(p.get(i,j) + ",")
        }
        println()
      }
      print("]")
    })
    println("Hello World!")
  }
}

