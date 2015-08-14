package org.dia.loaders


import ucar.nc2.dataset.NetcdfDataset

import scala.io.Source

object MergUtils {

  def ReadMergtoPair(shape: Array[Int])(offset: Double)(file: String, variable: String): (Array[Double], Array[Int]) = {
    val k = ReadMergtoJavaArray(file)
    val halfArray = for (i <- 0 to (k.length / 2)) yield k(i)
    (halfArray.toArray, shape)
  }

  def ReadMergtoJavaArray(file: String): Array[Double] = {
    val Sourcefile = Source.fromFile(file, "ISO8859-1")
    val byteArray = Sourcefile.map(_.toInt).toArray
    Sourcefile.close
    val SourceArray = byteArray.map(floatByte => floatByte.asInstanceOf[Float].toDouble)
    SourceArray
  }

  def ReadMergtoNetCDFDataset(file: String, shape: Array[Int]): NetcdfDataset = {
    null
  }
}