package org.dia.loaders


import scala.io.Source

object MergUtils {

  def ReadMergtoNDArray(shape: Array[Int], offset: Double)(file:String, varName:String) : (Array[Double], Array[Int]) = {
    val java1dArray = ReadMergtoJavaArray(file, offset, shape)
    (java1dArray, shape)
  }

  def ReadMergtoNDArray(file: String, shape: Array[Int], offset: Double): (Array[Double], Array[Int]) = {
    val java1dArray = ReadMergtoJavaArray(file, offset, shape)
    (java1dArray, shape)
  }

  def ReadMergtoJavaArray(file: String, offset: Double, shape:Array[Int]): Array[Double] = {
    val Sourcefile = Source.fromFile(file, "ISO8859-1")
    val numElems = shape.reduce(_*_)
    val byteArray = Sourcefile.map(_.toInt).slice(0, numElems).toArray
    Sourcefile.close()
    val SourceArray = byteArray.map(floatByte => floatByte.asInstanceOf[Float].toDouble + offset)
    SourceArray
  }

  def ReadMergByteArray(byteArray: Array[Byte], offset: Double, shape: Array[Int]): Array[Double] = {
    val numElems = shape.reduce(_ * _)
    val array = byteArray.slice(0, numElems)
    val SourceArray = byteArray.map(floatByte => floatByte.asInstanceOf[Float].toDouble + offset)
    SourceArray
  }

}
