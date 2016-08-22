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

import java.io._
import scala.collection.mutable
import scala.language.implicitConversions
import org.apache.spark.rdd.RDD
import org.dia.core.{SciSparkContext, SciTensor}

object NetcdfDFSMCC {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  val maxAreaOverlapThreshold = 0.66
  val minAreaOverlapThreshold = 0.50
  val minArea = 10000


  def create2DTempGrid(node: MCCNode, rowMax: Int, colMax: Int): Array[Array[Double]] = {
    val tempGrid = Array.ofDim[Double](rowMax, colMax)
    val gridMap: mutable.HashMap[String, Double] = node.getMetadata().
      get("grid").getOrElse().asInstanceOf[mutable.HashMap[String, Double]]
    gridMap.foreach { case (k, v) =>
      val indices = k.replace("(", "").replace(")", "").replace(" ", "").split(",")
      tempGrid(indices(0).toInt)(indices(1).toInt) = v
    }
    return tempGrid
  }

  def createMapFromEdgeList(edgesRDD: Iterable[MCCEdge],
                            lat: Array[Double], lon: Array[Double]): mutable.HashMap[String, Any] = {
    val MCCNodeMap = new mutable.HashMap[String, Any]()
    edgesRDD.foreach{edge => {
      val srcKey = s"${edge.srcNode.frameNum},${edge.srcNode.cloudElemNum}"
      val destKey = s"${edge.destNode.frameNum},${edge.destNode.cloudElemNum}"

      if (!MCCNodeMap.contains(srcKey)) {
        updateLatLongs(edge.srcNode, lat, lon)
        MCCNodeMap += ((srcKey, edge.srcNode))
      }
      if (!MCCNodeMap.contains(destKey)) {
        updateLatLongs(edge.destNode, lat, lon)
        MCCNodeMap += ((destKey, edge.destNode))
      }
    }}
    return MCCNodeMap
  }

  def updateLatLongs(node: MCCNode, lat: Array[Double], lon: Array[Double]): MCCNode = {
    val props: mutable.HashMap[String, Double] = node.metadata.get("properties")
      .getOrElse().asInstanceOf[mutable.HashMap[String, Double]]
    val xMax = props.get("rowMax").get
    val xMin = props.get("rowMin").get
    val yMax = props.get("colMax").get
    val yMin = props.get("colMin").get

    val latMax = lat(xMax.toInt)
    val latMin = lat(xMin.toInt)
    val lonMin = lon(yMin.toInt)
    val lonMax = lon(yMax.toInt)
    val centerLat = (latMax + latMin)/2
    val centerLon = (lonMax + lonMin)/2

    props += (("latMin", latMin))
    props += (("latMax", latMax))
    props += (("lonMin", lonMin))
    props += (("lonMax", lonMax))
    props += (("centerLat", centerLat))
    props += (("centerLon", centerLon))

    return node
  }

  /**
    * Given an RDD of consecutive frames, this function will find the edges meeting a specified criteria
    *
    * @param consecFrames
    * @param maxAreaOverlapThreshold: Max area for two cloud element to overlap and form an edge
    * @param minAreaOverlapThreshold : Min area for two cloud element to overlap and form an edge
    * @param minArea : Minimum area of a cloud element
    * @return
    */
  def findEdges(consecFrames: RDD[(SciTensor, SciTensor)], maxAreaOverlapThreshold: Double,
                minAreaOverlapThreshold: Double, minArea: Double): RDD[MCCEdge] = {
    val componentFrameRDD = consecFrames.flatMap({
    case (t1, t2) =>
      /**
        * First label the connected components in each pair.
        * The following example illustrates labeling.
        *
        * [0,1,2,0]       [0,1,1,0]
        * [1,2,0,0]   ->  [1,1,0,0]
        * [0,0,0,1]       [0,0,0,2]
        *
        * Note that a tuple of (Matrix, MaxLabel) is returned
        * to denote the labeled elements and the highest label.
        * This way only one traverse is necessary instead of a 2nd traverse
        * to find the highest label.
        */
      val (components1, _) = MCCOps.labelConnectedComponents(t1.tensor)
      val (components2, _) = MCCOps.labelConnectedComponents(t2.tensor)
      /**
        * The labeled components are element-wise multiplied
        * to find overlapping regions. Non-overlapping regions
        * result in a 0.
        *
        * [0,1,1,0]       [0,1,1,0]     [0,1,1,0]
        * [1,1,0,0]   X   [2,0,0,0]  =  [2,0,0,0]
        * [0,0,0,2]       [0,0,0,3]     [0,0,0,6]
        *
        */
      val product = components1 * components2
      /**
        * The overlappedPairsList keeps track of all points that
        * overlap between the labeled arrays. Note that the overlappedPairsList
        * will have several duplicate pairs if there are large regions of overlap.
        *
        * This is achieved by iterating through the product array and noting
        * all points that are not 0.
        */
      var overlappedPairsList = mutable.MutableList[((Double, Double))]() // (label1, label2)
      /**
        * The areaMinMaxTable keeps track of the area, minimum value, and maximum value
        * of all labeled regions in both components. For this reason the hash key has the following form :
        * 'F : C' where F = Frame Number and C = Component Number.
        * The areaMinMaxTable is updated by the updateComponent function, which is called in the for loop.
        *
        * @todo Extend it to have a lat long bounds for component.
        */
      var areaMinMaxTable = new mutable.HashMap[String, mutable.HashMap[String, Any]]

      val MCCEdgeList = new mutable.MutableList[MCCEdge]()
      def updateComponent(label: Double, frame: String, value: Double, row: Int, col: Int): Unit = {

        if (label != 0.0) {
          var area = 0.0
          var max = Double.MinValue
          var min = Double.MaxValue
          var rowMax = 0.0
          var colMax = 0.0
          var rowMin = Double.MaxValue
          var colMin = Double.MaxValue

          var grid = new mutable.HashMap[String, Double]()
          var currentProperties = new mutable.HashMap[String, String]()
          var metadata = new mutable.HashMap[String, Any]()
          if (areaMinMaxTable.contains(frame + ":" + label)) {
            metadata = areaMinMaxTable(frame + ":" + label)
            val currentPropertiesOption = metadata("properties")

            currentProperties = currentPropertiesOption match {
              case Some(anyProperty) => anyProperty.asInstanceOf[mutable.HashMap[String, String]]
              case None => throw new Exception("key properties not found")
            }

            val gridOption = metadata("grid")
            grid = gridOption match {
              case Some(anyProperty) => anyProperty.asInstanceOf[mutable.HashMap[String, Double]]
              case None => throw new Exception("key properties not found")
            }

            area = currentProperties("area").toDouble
            max = currentProperties("maxTemp").toDouble
            min = currentProperties("minTemp").toDouble
            rowMax = currentProperties("rowMax").toDouble
            colMax = currentProperties("colMax").toDouble
            rowMin = currentProperties("rowMin").toDouble
            colMin = currentProperties("colMin").toDouble

            if (value < min) {
              min = value
              currentProperties += (("minTemp", min.toString))
            }
            if (value > max) {
              max = value
              currentProperties += (("maxTemp", max.toString))
            }

          } else {
            currentProperties += (("minTemp", value.toString))
            currentProperties += (("maxTemp", value.toString))
          }
          rowMax = if (row > rowMax) row else rowMax
          colMax = if (col > colMax) col else colMax
          rowMin = if (row < rowMin) row else rowMin
          colMin = if (col < colMin) col else colMin
          area += 1
          currentProperties += (("area", area.toString))
          currentProperties += (("rowMax", rowMax.toString))
          currentProperties += (("colMax", colMax.toString))
          currentProperties += (("rowMin", rowMin.toString))
          currentProperties += (("colMin", colMin.toString))
          grid += ((s"($row, $col)", value))
          metadata += (("properties", currentProperties))
          metadata += (("grid", grid))
          areaMinMaxTable += ((frame + ":" + label, metadata))
        }
      }

      for (row <- 0 to product.rows - 1) {
        for (col <- 0 to product.cols - 1) {
          /** Find non-zero points in product array */
          if (product(row, col) != 0.0) {
            /** save components ids */
            val value1 = components1(row, col)
            val value2 = components2(row, col)
            overlappedPairsList += ((value1, value2))
          }
          updateComponent(components1(row, col), t1.metaData("FRAME"), t1.tensor(row, col), row, col)
          updateComponent(components2(row, col), t2.metaData("FRAME"), t2.tensor(row, col), row, col)
        }
      }

      /**
        * This code essentially computes the number of times the same tuple occurred in the list,
        * a repeat occurance would indicate that the components overlapped for more than one cell
        * in the product matrix. By calculating the number of overlaps we can calculate the number of cells
        * they overlapped for and since each cell is of a fixed area we can compute the area overlap
        * between those two components.
        */
      var overlappedMap = overlappedPairsList.groupBy(identity).mapValues(_.size)

      /**
        * Once the overlapped pairs have been computed, eliminate all duplicates
        * by converting the collection to a set. The component edges are then
        * mapped to the respective frames, so the global space of edges (outside of this task)
        * consists of unique tuples.
        */
      val edgesSet = overlappedPairsList.toSet

      val edges = edgesSet.map({ case (c1, c2) => ((t1.metaData("FRAME"), c1), (t2.metaData("FRAME"), c2)) })

    val filtered = edges.filter({
      case ((frameId1, compId1), (frameId2, compId2)) =>
        val frame_component1 = frameId1 + ":" + compId1
        val cloud1 = areaMinMaxTable(frame_component1)("properties").asInstanceOf[mutable.HashMap[String, Double]]
        val (area1, min1, max1) = (cloud1("area"), cloud1("minTemp"), cloud1("maxTemp"))
        val isCloud1 = ((area1 >= 2400.0) || ((area1 < 2400.0) && ((min1 / max1) < 0.9)))
        val frame_component2 = frameId2 + ":" + compId2
        val cloud2 = areaMinMaxTable(frame_component2)("properties").asInstanceOf[mutable.HashMap[String, Double]]
        val (area2, max2, min2) = (cloud2("area"), cloud2("minTemp"), cloud2("maxTemp"))
        val isCloud2 = ((area2 >= 2400.0) || ((area2 < 2400.0) && ((min2 / max2) < 0.9)))
        var meetsCriteria = true
        if (isCloud1 && isCloud2) {
          val areaOverlap = overlappedMap(compId1, compId2)
          val percentAreaOverlap = math.max((areaOverlap / area1), (areaOverlap / area2))
          var edgeWeight = 0
          if (percentAreaOverlap >= maxAreaOverlapThreshold) {
            edgeWeight = 1
          }
          else if (percentAreaOverlap < maxAreaOverlapThreshold &&
            percentAreaOverlap >= minAreaOverlapThreshold) {
            edgeWeight = 2
          }
          else if (areaOverlap >= minArea) {
            edgeWeight = 3
          }
          else {
            meetsCriteria = false
          }
          overlappedMap += (((compId1, compId2), edgeWeight))
          if (meetsCriteria) {
            var node1: MCCNode = new MCCNode(frameId1.toInt, compId1.toFloat)
            val metadata1 = areaMinMaxTable(frameId1 + ":" + compId1)
            val currentPropertiesOption1 = metadata1("properties")
            val currentProperties1 = currentPropertiesOption1 match {
              case Some(anyProperty) => anyProperty.asInstanceOf[mutable.HashMap[String, String]]
              case None => throw new Exception("key properties not found")
            }
            val gridOption1 = metadata1("grid")
            val grid1 = gridOption1 match {
              case Some(anyProperty) => anyProperty.asInstanceOf[mutable.HashMap[String, Double]]
              case None => throw new Exception("key properties not found")
            }
            node1.setMetadata(currentProperties1)
            node1.setGrid(grid1)

            var node2: MCCNode = new MCCNode(frameId2.toInt, compId2.toFloat)
            val metadata2 = areaMinMaxTable(frameId2 + ":" + compId2)
            val currentPropertiesOption2 = metadata2("properties")
            val currentProperties2 = currentPropertiesOption2 match {
              case Some(anyProperty) => anyProperty.asInstanceOf[mutable.HashMap[String, String]]
              case None => throw new Exception("key properties not found")
            }
            val gridOption2 = metadata2("grid")
            val grid2 = gridOption2 match {
              case Some(anyProperty) => anyProperty.asInstanceOf[mutable.HashMap[String, Double]]
              case None => throw new Exception("key properties not found")
            }
            node2.setMetadata(currentProperties2)
            node2.setGrid(grid2)

            MCCEdgeList += new MCCEdge(node1, node2, edgeWeight)
          }
        }
        isCloud1 && isCloud2 && meetsCriteria
    })
      MCCEdgeList
  })
    return componentFrameRDD
  }

  /**
    * For each array N* where N is the frame number and N* is the array
    * output the following pairs (N, N*), (N + 1, N*).
    *
    * After flat-mapping the pairs and applying additional pre-processing
    * we have pairs (X, Y) where X is a label and Y a tensor.
    *
    * After grouping by X and reordering pairs we obtain pairs
    * (N*, (N+1)*) which achieves the consecutive pairwise grouping
    * of frames.
    */
  def groupConsecutiveFrames(sRDD: RDD[SciTensor]): RDD[(SciTensor, SciTensor)] = {
    val consecFrames = sRDD.flatMap(p => {
      List((p.metaData("FRAME").toInt, p), (p.metaData("FRAME").toInt + 1, p))
    }).groupBy(_._1)
      .map(p => p._2.map(e => e._2).toList)
      .filter(p => p.size > 1)
      .map(p => p.sortBy(_.metaData("FRAME").toInt))
      .map(p => (p(0), p(1)))
    return consecFrames
  }

  def createLabelledRDD(sRDD: RDD[SciTensor]): RDD[SciTensor] = {
    val labeled = sRDD.map(p => {
      val source = p.metaData("SOURCE").split("/").last.split("_")(1)
      val FrameID = source.toInt
      p.insertDictionary(("FRAME", FrameID.toString))
      p.insertVar(p.varInUse, p()(0))
      p
    })
    return labeled
  }

  def main(args: Array[String]): Unit = {
    /**
     * Input arguments to the program :
     * args(0) - the spark master URL. Example : spark://HOST_NAME:7077
     * args(1) - the number of desired partitions. Default : 2
     * args(2) - square matrix dimension. Default : 20
     * args(3) - variable name
     * args(4) - local path to files
     *
     */
    val masterURL = if (args.isEmpty) "local[2]" else args(0)
    val partCount = if (args.length <= 1) 2 else args(1).toInt
    val dimension = if (args.length <= 2) (20, 20) else (args(2).toInt, args(2).toInt)
    val variable = if (args.length <= 3) "ch4" else args(3)
    val hdfspath = if (args.length <= 4) "resources/merg" else args(4)
    val maxAreaOverlapThreshold = 0.66
    val minAreaOverlapThreshold = 0.50
    val minArea = 10000

    val outerTemp = 240.0
    val innerTemp = 220.0
    println("Starting MCC")

    /**
     * Initialize the spark context to point to the master URL
     */
    val sc = new SciSparkContext(masterURL, "DGTG : Distributed MCC Search")

    /**
     * Ingest the input file and construct the SRDD.
     * For MCC the sources are used to map date-indexes.
     * The metadata variable "FRAME" corresponds to an index.
     * The indices themselves are numbered with respect to
     * date-sorted order.
     *
     * Note if no HDFS path is given, then randomly generated matrices are used.
     *
     */
    val sRDD = sc.NetcdfDFSFiles(hdfspath, List("ch4", "longitude", "latitude"), partCount)
    val labeled = createLabelledRDD(sRDD)

    val collected = labeled.collect()
    val lon = collected(0).variables("longitude").data
    val lat = collected(0).variables("latitude").data
//    val collected_filtered = collected.map(p => p(variable) <= 241.0)
    val filtered = labeled.map(p => p(variable) <= 241.0)

    val consecFrames = groupConsecutiveFrames(filtered)

    val edgesRDD = findEdges(consecFrames, maxAreaOverlapThreshold,
      minAreaOverlapThreshold, minArea).collect()
    val MCCNodeMap = createMapFromEdgeList(edgesRDD, lat, lon)
    //    val json = new JSONObject(MCCNodeMap.toMap)
    val pw = new PrintWriter("MCCNodesLines.json")
    MCCNodeMap.foreach { case (key, value) =>
      pw.write(value.toString)
      pw.write("\n")
    }
    pw.close()

//    val fw = new FileWriter("MCCEdges.txt")
//    fw.write(MCCEdgeList.toString())
//    fw.close()


  }
}

