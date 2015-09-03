package Perf

import breeze.linalg.DenseMatrix
import org.nd4j.linalg.factory.Nd4j


/**
 * Created by rahulsp on 9/1/15.
 */

object Performance {

  def main(args: Array[String]): Unit = {
    val dim = if (!args.isEmpty) args(0).toInt else 10
    val breezeArray1 = DenseMatrix.eye[Double](10)
    val breezeArray2 = DenseMatrix.eye[Double](10)
    println(breezeArray1 + breezeArray2)
    println(breezeArray1 * breezeArray2)
    val nd4jArray1 = Nd4j.eye(10)
    val nd4jArray2 = Nd4j.eye(10)
    println(nd4jArray1 add nd4jArray2)
    println(nd4jArray1 mmul nd4jArray2)

    println("Warmed Up")
    println("Beginning Elementwise Tests : Breeze")

    for (i <- 1 to dim) {
      val breezeArray3 = DenseMatrix.eye[Double](i * 1000)
      val breezeArray4 = DenseMatrix.eye[Double](i * 1000)

      val start = System.nanoTime()
      val breezeSum = breezeArray3 + breezeArray4
      val stop = System.nanoTime()
      println(stop - start)
    }

    println("Beginning Elementwise Tests : Nd4j")
    for (i <- 1 to dim) {
      val nd4jArray3 = Nd4j.eye(i * 1000)
      val nd4jArray4 = Nd4j.eye(i * 1000)

      val start = System.nanoTime()
      val nd4jSum = nd4jArray3 add nd4jArray4
      val stop = System.nanoTime()
      println(stop - start)
    }


    println("Beginning Vectorwise Tests : Breeze")

    for (i <- 1 to dim) {
      val breezeArray3 = DenseMatrix.eye[Double](i * 1000)
      val breezeArray4 = DenseMatrix.eye[Double](i * 1000)

      val start = System.nanoTime()
      val breezeSum = breezeArray3 * breezeArray4
      val stop = System.nanoTime()
      println(stop - start)
    }

    println("Beginning Vectorwise Tests : Nd4j")
    for (i <- 1 to dim) {
      val nd4jArray3 = Nd4j.eye(i * 1000)
      val nd4jArray4 = Nd4j.eye(i * 1000)

      val start = System.nanoTime()
      val nd4jSum = nd4jArray3 mmul nd4jArray4
      val stop = System.nanoTime()
      println(stop - start)
    }
  }
}

