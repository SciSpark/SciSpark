package org.dia.loaders


import scala.io.Source

/**
 * Utility functions to read binary data from MERG files.
 * Note that MERG files by default store floating point numbers
 * in unsigned bytes.
 */
object MergReader {

  def LoadMERGArray(shape: Array[Int], offset: Double)(file: String, varName: String): (Array[Double], Array[Int]) = {
    val java1dArray = ReadMergtoJavaArray(file, offset, shape)
    (java1dArray, shape)
  }

  def LoadMERGArray(file: String, shape: Array[Int], offset: Double): (Array[Double], Array[Int]) = {
    val java1dArray = ReadMergtoJavaArray(file, offset, shape)
    (java1dArray, shape)
  }

  def ReadMergtoJavaArray(file: String, offset: Double, shape: Array[Int]): Array[Double] = {
    val Sourcefile = Source.fromFile(file, "ISO8859-1")
    val numElems = shape.product
    val byteArray = Sourcefile.map(_.toInt).slice(0, numElems).toArray
    Sourcefile.close()
    val SourceArray = byteArray.map(floatByte => (floatByte & 0xff).asInstanceOf[Float].toDouble + offset)
    SourceArray
  }

  def ReadMergByteArray(byteArray: Array[Byte], offset: Double, shape: Array[Int]): Array[Double] = {
    val numElems = shape.product
    val array = byteArray.slice(0, numElems)
    val SourceArray = byteArray.map(floatByte => (floatByte & 0xff).asInstanceOf[Float].toDouble + offset)
    SourceArray
  }

}
