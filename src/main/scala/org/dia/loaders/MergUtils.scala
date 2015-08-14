package org.dia.loaders


import scala.io.Source

object MergUtils {

  def ReadMergtoNDArray(file: String, shape: Array[Int], offset: Double): (Array[Double], Array[Int]) = {
    val java1dArray = ReadMergtoJavaArray(file)
    (java1dArray, shape)
  }

  def ReadMergtoJavaArray(file: String): Array[Double] = {
    val Sourcefile = Source.fromFile(file, "ISO8859-1")
    val byteArray = Sourcefile.map(_.toInt).toArray
    Sourcefile.close()
    val SourceArray = byteArray.map(floatByte => floatByte.asInstanceOf[Float].toDouble)
    SourceArray
  }

}
