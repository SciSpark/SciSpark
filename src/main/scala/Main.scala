import org.apache.spark.mllib.linalg.{Matrix, Matrices}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg

import org.jblas.DoubleMatrix

import ucar.nc2.Variable
import ucar.nc2.dataset.NetcdfDataset
import ucar.nc2.NetcdfFile

import scala.collection.JavaConversions
import scala.io.Source
/**
 * Created by rahulsp on 6/17/15.
 */
object Main {

  val TotCldLiqH2O = "TotCldLiqH2O_A"
  def getNetCDFVars (url : String, variable : String) : DoubleMatrix = {
    val netcdfFile = NetcdfDataset.openDataset(url);
    //if(netcdfFile == null) println("UHOH");
    val SearchVariable = netcdfFile.findVariable(variable).read()
    val coordinateArray = SearchVariable.copyTo1DJavaArray().asInstanceOf[Array[Float]].map(p => p.toDouble)
    //doubleCoordinateArray
    val matrix = new DoubleMatrix(coordinateArray).reshape(180, 360)
    matrix
  }

  def decomposeArray(largeArray : DoubleMatrix, magnifyDim : Int) : DoubleMatrix =  {
    val numRows = largeArray.columns
    val numCols = largeArray.rows

    val reducedSize = numRows * numCols / (magnifyDim * magnifyDim)

    val reducedMatrix = DoubleMatrix.zeros(numRows / magnifyDim, numCols / magnifyDim)
    for(row <- 0 to numRows){
      for(col <- 0 to numCols){
        println(row + ";" + col)
        val smallRow = row / magnifyDim
        val smallCol = col / magnifyDim
        val initial = reducedMatrix.get(smallRow, smallCol)
        val update = initial + (largeArray.get(row, col) / (magnifyDim * magnifyDim).toDouble)
        reducedMatrix.put(row / magnifyDim, col / magnifyDim, update)
      }
    }

    reducedMatrix
  }

  def main(args : Array[String]) : Unit = {
    val conf = new SparkConf().setAppName("L").setMaster("local[4]")
    val sparkContext = new SparkContext(conf)
    val urlRDD = sparkContext.textFile("Links").repartition(4)
    println("Staring urlMapping")
    val HighResolutionArray = urlRDD.map(url => getNetCDFVars(url, TotCldLiqH2O))
    println("Staring decomposition")
    val LowResolutionArray = HighResolutionArray.map(largeArray => decomposeArray(largeArray, 5))
    println(HighResolutionArray.count)

    HighResolutionArray.collect.map(p => println(p.length))
    println("Hello World!")
  }
}

