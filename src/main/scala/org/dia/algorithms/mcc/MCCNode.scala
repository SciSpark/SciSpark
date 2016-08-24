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

class MCCNode(var frameNum: Int, var cloudElemNum: Double) extends Serializable {

  var inEdges: mutable.HashSet[MCCEdge] = new mutable.HashSet[MCCEdge]
  var outEdges: mutable.HashSet[MCCEdge] = new mutable.HashSet[MCCEdge]
  var metadata: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
  var grid: mutable.HashMap[String, Double] = new mutable.HashMap[String, Double]()
  var area: Double = 0.0
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

  def getRowMax(): Int = {
    return rowMax
  }

  def getRowMin(): Int = {
    return rowMin
  }

  def getColMax(): Int = {
    return colMax
  }

  def getColMin(): Int = {
    return colMin
  }

  def getLatMax(): Double = {
    return latMax
  }

  def getLatMin(): Double = {
    return latMin
  }

  def getLonMax(): Double = {
    return lonMax
  }

  def getLonMin(): Double = {
    return lonMin
  }

  def getCenterLat(): Double = {
    return centerLat
  }

  def getCenterLon(): Double = {
    return centerLon
  }

  def getArea(): Double = {
    return area
  }

  def getMaxTemp(): Double = {
    return maxTemp
  }

  def getMinTemp(): Double = {
    return minTemp
  }

  def setGrid(_grid: mutable.HashMap[String, Double]): Unit = {
    grid = _grid
  }

  /**
    * To be used for printing purposes only,
    * to update the grid use updateGrid() method.
    * This methods returns a Java Map
    * @return
    */
  def getGrid(): util.Map[String, Double] = {
    return grid.asJava
  }

  def updateGrid(key: String, value: Double): Unit = {
    grid.update(key, value)
  }

  def setMetadata(_metadata: mutable.HashMap[String, String]): Unit = {
    metadata = _metadata
  }

  /**
    * To be used for printing purposes only,
    * to update the metadata use updateMetadata() method.
    * This method returns a Java Map
    * @return
    */
  def getMetadata(): util.Map[String, String] = {
    return metadata.asJava
  }

  def updateMetadata(key: String, value: String): Unit = {
    metadata.update(key, value)
  }

  def connectTo(destNode: MCCNode, weight: Double): MCCEdge = {
    val edge = new MCCEdge(this, destNode, weight)
    addOutgoingEdge(edge)
    return edge
  }

  def connectFrom(srcNode: MCCNode, weight: Double): MCCEdge = {
    val edge = new MCCEdge(this, srcNode, weight)
    addIncomingEdge(edge)
    return edge
  }

  def addIncomingEdge(edge: MCCEdge): mutable.HashSet[MCCEdge] = {
    inEdges += edge
  }

  def addOutgoingEdge(edge: MCCEdge): mutable.HashSet[MCCEdge] = {
    outEdges += edge
  }

  def getFrameNum: Int = {
    frameNum
  }

  def getCloudElemNum: Double = {
    cloudElemNum
  }

  def setFrameNum(f: Int): Unit = {
    frameNum = f
  }

  def setCloudElemNum(c: Float): Unit = {
    cloudElemNum = c
  }

  def update(value: Double, row: Int, col: Int ): Unit = {
    updateRowAndCol(row, col)
    updateTemperatures(value)
    area += 1
    grid += ((s"($row, $col)", value))
  }

  def updateRowAndCol(row: Int, col: Int): Unit = {
    rowMax = if (row > rowMax) row else rowMax
    colMax = if (col > colMax) col else colMax
    rowMin = if (row < rowMin) row else rowMin
    colMin = if (col < colMin) col else colMin
  }

  def updateTemperatures(value: Double): Unit = {
    minTemp = if (value < minTemp) value else minTemp
    maxTemp = if (value > maxTemp) value else maxTemp
  }

  def updateLatLon(lat: Array[Double], lon: Array[Double]): Unit = {
    latMax = lat(rowMax)
    latMin = lat(rowMin)
    lonMax = lon(colMax)
    lonMin = lon(colMin)

    centerLat = (latMax + latMin) / 2
    centerLon = (lonMax + lonMin) / 2
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
