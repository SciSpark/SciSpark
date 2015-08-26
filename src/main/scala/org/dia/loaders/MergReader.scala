package org.dia.loaders


import scala.io.Source

/**
 * Utility functions to read binary data from MERG files.
 * Note that MERG files by default store floating point numbers
 * in unsigned bytes.
 *
 * Note that 3 copies of the array are created in memory with these calls,
 * primarily due to the conversion from byte arrays to Double arrays.
 * 1) The file or byte array
 * 2) The slice of the array needed, since MERG files 30 minute intervals in hourly files.
 * 3) The mapped conversion to Doubles
 *
 * 2n + 8n = 10n where n corresponds to the number of bytes.
 * The last 8n is due to the double conversion.
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

  def ReadMergBytetoJavaArray(byteArray: Array[Byte], offset: Double, shape: Array[Int]): Array[Double] = {
    val numElems = shape.product
    val array = byteArray.slice(0, numElems)
    val SourceArray = byteArray.map(floatByte => (floatByte & 0xff).asInstanceOf[Float].toDouble + offset)
    SourceArray
  }

}
