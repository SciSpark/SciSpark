/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.dia.utils

import java.io.{File, FileWriter, PrintWriter, Writer}

import scala.language.reflectiveCalls

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
/**
 * Utilities to read from/write to files.
 */
object FileUtils {

  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Used for reading/writing to a database, files, etc.
   * Code from the book "Beginning Scala" from David Pollak.
   */
  def using[A <: Writer, B](param: A)(f: A => B): B =
    try {
      f(param)
    } finally {
      param.close()
    }

  /**
   * @param fileName String for filename on local system to write data to
   * @param data String of the data to be written to file
   */
  def writeToFile(fileName: String, data: String): Unit =
    using(new FileWriter(fileName)) {
      fileWriter => fileWriter.write(data)
    }

  /**
   * @param fileName String for filename on local system to append data to
   * @param textData String of data to be appended to file
   */
  def appendToFile(fileName: String, textData: String): Unit =
    using(new FileWriter(fileName, true)) {
      fileWriter =>
        using(new PrintWriter(fileWriter)) {
          printWriter => printWriter.println(textData)
        }
    }

  /**
   * Copy file to hdfs. NB: file is deleted from local FS
   * @param hdfsDir  String The HDFS directory
   * @param localFile The filename on the local directory to be copied
   */
  def copyFileToHDFS(
      hdfsDir: String,
      localFile: String): Unit = {
    try {
      val dstPath = new Path(hdfsDir)
      val conf = new Configuration()
      val fs = FileSystem.get(dstPath.toUri, conf)
      val srcPath = new Path(localFile)
      fs.copyFromLocalFile(srcPath, dstPath)
      new File(localFile).delete()
    }
    catch {
      case _: Throwable => logger.info("Error copying " + localFile + " to HDFS. \n")
    }
  }

  /**
   * Copy files from hdfs to localDir
   * @param hdfsDir  String The hdfs directory
   * @param localDir The local directory
   */
  def copyFilesToHDFS(
      hdfsDir: String,
      localDir: String): Unit = {
    try {
      val conf = new Configuration()
      val srcPath = new File(localDir)
      var currLink = ""
      // val allFiles = FileSystem.get(sc.hadoopConfiguration).listFiles(new Path(hdfsDir), true)
      val allFiles = srcPath.listFiles.filter(_.isFile).toList
      while (allFiles.hasNext()) {
        currLink = allFiles.next().getPath().toString
        copyFileToHDFS(hdfsDir, currLink)
      }
    }
    catch {
      case _: Throwable => logger.info("Error copying from HDFS location " + hdfsDir + "\n")
    }
  }

  /**
   * Copy file from hdfs to localDir
   * @param hdfsFile String The full hdfsFile path with filename
   * @param localDir The local directory
   */
  def copyFileFromHDFS(
      hdfsFile: String,
      localDir: String): Unit = {
    try {
      val dstPath = new Path(localDir)
      val conf = new Configuration()
      val fs = FileSystem.get(dstPath.toUri, conf)
      val srcPath = new Path(hdfsFile)
      fs.copyToLocalFile(srcPath, dstPath)
    }
    catch {
      case _: Throwable => logger.info("Error copying " + hdfsFile + " from HDFS. \n")
    }
  }

  /**
   * Copy files from hdfs to localDir
   * @param hdfsDir  String The hdfs directory
   * @param localDir The local directory
   */
  def copyFilesFromHDFS(
      hdfsDir: String,
      localDir: String): Unit = {
    try {
      val conf = new Configuration()
      val srcPath = new Path(hdfsDir)
      var currHDFSLink = ""
      val allFiles = FileSystem.get(srcPath.toUri, conf).listFiles(srcPath, true)
      while (allFiles.hasNext()) {
        currHDFSLink = allFiles.next().getPath().toString
        copyFileFromHDFS(currHDFSLink, localDir)
      }
    }
    catch {
      case _: Throwable => logger.info("Error copying from HDFS location " + hdfsDir + "\n")
    }
  }
}
