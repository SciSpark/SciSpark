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
package org.dia.algorithms.mcc

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import scala.collection.JavaConverters._
import scala.collection.mutable

class MCCNode(var frameNum: Int, var cloudElemNum: Int) extends Serializable {

  var inEdges: mutable.HashSet[MCCEdge] = new mutable.HashSet[MCCEdge]
  var outEdges: mutable.HashSet[MCCEdge] = new mutable.HashSet[MCCEdge]
  var metadata: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
  var grid: mutable.HashMap[String, Double] = new mutable.HashMap[String, Double]()
  var area: Int = 0
  var rowMax: Int = 0
  var rowMin: Int = 0
  var colMax: Int = 0
  var colMin: Int = 0
  var latMax: Double = 0.0
  var latMin: Double = 0.0
  var lonMax: Double = 0.0
  var lonMin: Double = 0.0
  var centerLat: Double = 0.0
  var centerLon: Double = 0.0

  /** Since temperature is in Kelvin, we don't want to go below absolute zero */
  var maxTemp: Double = 0.0
  var minTemp: Double = Double.MaxValue

  def this(frameNum: String, cloudElemNum: String) {
    this(frameNum.toInt, cloudElemNum.toInt)
  }
  def this(frameNum: String, cloudElemNum: Double) {
    this(frameNum.toInt, cloudElemNum.toInt)
  }

  def getRowMax(): Int = {
     this.rowMax
  }

  def getRowMin(): Int = {
    this.rowMin
  }

  def getColMax(): Int = {
    this.colMax
  }

  def getColMin(): Int = {
    this.colMin
  }

  def getLatMax(): Double = {
    this.latMax
  }

  def getLatMin(): Double = {
    this.latMin
  }

  def getLonMax(): Double = {
    this.lonMax
  }

  def getLonMin(): Double = {
    this.lonMin
  }

  def getCenterLat(): Double = {
    this.centerLat
  }

  def getCenterLon(): Double = {
    this.centerLon
  }

  def getArea(): Double = {
    this.area
  }

  def getMaxTemp(): Double = {
    this.maxTemp
  }

  def getMinTemp(): Double = {
    this.minTemp
  }

  def setGrid(_grid: mutable.HashMap[String, Double]): Unit = {
    this.grid = _grid
  }

  /**
   * To be used for printing purposes only,
   * to update the grid use updateGrid() method.
   * This methods returns a Java Map
   *
   * @return
   */
  def getGrid(): util.Map[String, Double] = {
    this.grid.asJava
  }

  def updateGrid(key: String, value: Double): Unit = {
    this.grid.update(key, value)
  }

  def setMetadata(_metadata: mutable.HashMap[String, String]): Unit = {
    this.metadata = _metadata
  }

  /**
   * To be used for printing purposes only,
   * to update the metadata use updateMetadata() method.
   * This method returns a Java Map
   *
   * @return
   */
  def getMetadata(): util.Map[String, String] = {
     metadata.asJava
  }

  def updateMetadata(key: String, value: String): Unit = {
    this.metadata.update(key, value)
  }

  def connectTo(destNode: MCCNode, weight: Double): MCCEdge = {
    val edge = new MCCEdge(this, destNode, weight)
    addOutgoingEdge(edge)
     edge
  }

  def connectFrom(srcNode: MCCNode, weight: Double): MCCEdge = {
    val edge = new MCCEdge(this, srcNode, weight)
    addIncomingEdge(edge)
     edge
  }

  def addIncomingEdge(edge: MCCEdge): mutable.HashSet[MCCEdge] = {
    this.inEdges += edge
  }

  def addOutgoingEdge(edge: MCCEdge): mutable.HashSet[MCCEdge] = {
    this.outEdges += edge
  }

  def getFrameNum: Int = {
    this.frameNum
  }

  def getCloudElemNum: Double = {
    this.cloudElemNum
  }

  def setFrameNum(f: Int): Unit = {
    this.frameNum = f
  }

  def setCloudElemNum(c: Int): Unit = {
    this.cloudElemNum = c
  }

  def update(value: Double, row: Int, col: Int): Unit = {
    updateRowAndCol(row, col)
    updateTemperatures(value)
    this.area += 1
    this.grid += ((s"($row, $col)", value))
  }

  def updateRowAndCol(row: Int, col: Int): Unit = {
    this.rowMax = if (row > this.rowMax) row else this.rowMax
    this.colMax = if (col > this.colMax) col else this.colMax
    this.rowMin = if (row < this.rowMin) row else this.rowMin
    this.colMin = if (col < this.colMin) col else this.colMin
  }

  def updateTemperatures(value: Double): Unit = {
    this.minTemp = if (value < this.minTemp) value else this.minTemp
    this.maxTemp = if (value > this.maxTemp) value else this.maxTemp
  }

  def updateLatLon(lat: Array[Double], lon: Array[Double]): Unit = {
    this.latMax = lat(this.rowMax)
    this.latMin = lat(this.rowMin)
    this.lonMax = lon(this.colMax)
    this.lonMin = lon(this.colMin)

    this.centerLat = (this.latMax + this.latMin) / 2
    this.centerLon = (this.lonMax + this.lonMin) / 2
  }

  override def toString(): String = {
    val mapper = new ObjectMapper()
    s"${mapper.writeValueAsString(this)}"
  }

  override def equals(that: Any): Boolean = that match {
    case that: MCCNode => that.frameNum == this.frameNum && that.cloudElemNum == this.cloudElemNum
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()
}
