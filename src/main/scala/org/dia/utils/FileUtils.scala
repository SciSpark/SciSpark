package org.dia.utils

import java.io.{ FileWriter, PrintWriter }
import scala.language.reflectiveCalls

/**
 * Utilities to read from/write to files.
 */
object FileUtils {

  /**
   * Used for reading/writing to a database, files, etc.
   * Code from the book "Beginning Scala" from David Pollak.
   */
  def using[A <: { def close(): Unit }, B](param: A)(f: A => B): B =
    try { f(param) } finally { param.close() }

  def writeToFile(fileName: String, data: String) =
    using(new FileWriter(fileName)) {
      fileWriter => fileWriter.write(data)
    }

  def appendToFile(fileName: String, textData: String) =
    using(new FileWriter(fileName, true)) {
      fileWriter =>
        using(new PrintWriter(fileWriter)) {
          printWriter => printWriter.println(textData)
        }
    }

}
