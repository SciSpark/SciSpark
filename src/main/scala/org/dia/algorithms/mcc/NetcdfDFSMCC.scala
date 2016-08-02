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
import java.text.SimpleDateFormat
import com.fasterxml.jackson.module.scala.JacksonModule
import org.apache.spark.rdd.RDD
import org.dia.Parsers
import org.dia.core.{SciSparkContext, SciTensor}
import org.slf4j.Logger
import scala.collection.mutable
import scala.io.Source
import scala.language.implicitConversions
import scala.util.parsing.json.JSONObject


object NetcdfDFSMCC {
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  val maxAreaOverlapThreshold = 0.66
  val minAreaOverlapThreshold = 0.50
  val minArea = 10000

  def create2DTempGrid(node: MCCNode, rowMax: Int, colMax: Int): Array[Array[Double]] = {
    val tempGrid = Array.ofDim[Double](rowMax, colMax)
    val gridMap: mutable.HashMap[String, Double] = node.getMetadata().
      get("grid").getOrElse().asInstanceOf[mutable.HashMap[String, Double]]
    gridMap.foreach{case(k,v) => {
      val indices = k.replace("(", "").replace(")","").replace(" ", "").split(",")
      tempGrid(indices(0).toInt)(indices(1).toInt) = v
    }}
    return tempGrid
  }

  def removeOverlappingEdges(t1: SciTensor, t2: SciTensor) = {
    val (components1, _) = MCCOps.labelConnectedComponents(t1.tensor)
    val (components2, _) = MCCOps.labelConnectedComponents(t2.tensor)
    val product = components1 * components2
    /**
      * The overlappedPairsList keeps track of all points that
      * overlap between the labeled arrays. Note that the overlappedPairsList
      * will have several duplicate pairs if there are large regions of overlap.
      *
      * This is achieved by iterating through the product array and noting
      * all points that are not 0.
      */
    var overlappedPairsList = mutable.MutableList[((Double, Double))]()
    /**
      * The areaMinMaxTable keeps track of the area, minimum value, and maximum value
      * of all labeled regions in both components. For this reason the hash key has the following form :
      * 'F : C' where F = Frame Number and C = Component Number.
      * The areaMinMaxTable is updated by the updateComponent function, which is called in the for loop.
      *
      * @todo Extend it to have a lat long bounds for component.
      */
    var areaMinMaxTable = new mutable.HashMap[String, (Double, Double, Double)]

    def updateComponent(label: Double, frame: String, value: Double): Unit = {

      if (label != 0.0) {
        var area = 0.0
        var max = Double.MinValue
        var min = Double.MaxValue
        val currentProperties = areaMinMaxTable.get(frame + ":" + label)
        if (currentProperties != null && currentProperties.isDefined) {
          area = currentProperties.get._1
          max = currentProperties.get._2
          min = currentProperties.get._3
          if (value < min) min = value
          if (value > max) max = value
        } else {
          min = value
          max = value
        }
        area += 1
        areaMinMaxTable += ((frame + ":" + label, (area, max, min)))
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
        updateComponent(components1(row, col), t1.metaData("FRAME"), t1.tensor(row, col))
        updateComponent(components2(row, col), t2.metaData("FRAME"), t2.tensor(row, col))
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
    println(s"Overlap Map ${overlappedMap.size}")

    /**
      * Once the overlapped pairs have been computed, eliminate all duplicates
      * by converting the collection to a set. The component edges are then
      * mapped to the respective frames, so the global space of edges (outside of this task)
      * consists of unique tuples.
      */
    val edgesSet = overlappedPairsList.toSet
    println(s"Overlap Set ${edgesSet.size}  : ") // for debugging
  }

  def findEdges(consecFrames: RDD[(SciTensor, SciTensor)]): RDD[((String, Double), (String, Double), Int)] = {
    val componentFrameRDD = consecFrames.flatMap({
      case (t1, t2) => {
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
        var overlappedPairsList = mutable.MutableList[((Double, Double))]()
        /**
          * The areaMinMaxTable keeps track of the area, minimum value, and maximum value
          * of all labeled regions in both components. For this reason the hash key has the following form :
          * 'F : C' where F = Frame Number and C = Component Number.
          * The areaMinMaxTable is updated by the updateComponent function, which is called in the for loop.
          *
          * @todo Extend it to have a lat long bounds for component.
          */
        var areaMinMaxTable = new mutable.HashMap[String, (Double, Double, Double)]

        def updateComponent(label: Double, frame: String, value: Double): Unit = {

          if (label != 0.0) {
            var area = 0.0
            var max = Double.MinValue
            var min = Double.MaxValue
            val currentProperties = areaMinMaxTable.get(frame + ":" + label)
            if (currentProperties != null && currentProperties.isDefined) {
              area = currentProperties.get._1
              max = currentProperties.get._2
              min = currentProperties.get._3
              if (value < min) min = value
              if (value > max) max = value
            } else {
              min = value
              max = value
            }
            area += 1
            areaMinMaxTable += ((frame + ":" + label, (area, max, min)))
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
            updateComponent(components1(row, col), t1.metaData("FRAME"), t1.tensor(row, col))
            updateComponent(components2(row, col), t2.metaData("FRAME"), t2.tensor(row, col))
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
        println(s"Overlap Map ${overlappedMap.size}")

        /**
          * Once the overlapped pairs have been computed, eliminate all duplicates
          * by converting the collection to a set. The component edges are then
          * mapped to the respective frames, so the global space of edges (outside of this task)
          * consists of unique tuples.
          */
        val edgesSet = overlappedPairsList.toSet
        println(s"Overlap Set ${edgesSet.size}  : ") // for debugging

        val edges = edgesSet.map({ case (c1, c2) => ((t1.metaData("FRAME"), c1), (t2.metaData("FRAME"), c2)) })
        println(s"Edges ${edges.size} ") // for debugging
        val filtered = edges.filter({
            case ((frameId1, compId1), (frameId2, compId2)) => {
              val (area1, max1, min1) = areaMinMaxTable(frameId1 + ":" + compId1)
              val isCloud1 = ((area1 >= 2400.0) || ((area1 < 2400.0) && ((min1 / max1) < 0.9)))
              val (area2, max2, min2) = areaMinMaxTable(frameId2 + ":" + compId2)
              val isCloud2 = ((area2 >= 2400.0) || ((area2 < 2400.0) && ((min2 / max2) < 0.9)))
              var meetsCriteria = true
              if (isCloud1 && isCloud2) {
                val areaOverlap = overlappedMap.get(compId1, compId2).get
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
                overlappedMap += (((compId1, compId2), 1))
              }
              isCloud1 && isCloud2 && meetsCriteria
            }
          })

        val edgeList = new mutable.HashSet[((String, Double), (String, Double), Int)]()
        filtered.foreach(edge => {
          val key = (edge._1._2, edge._2._2)
          if (overlappedMap.contains(key)) {
            edgeList += ((edge._1, edge._2, overlappedMap.get(key).get))
          }
        })
        println(s"edgeList Map filetered ${edgeList.size}: $edgeList")
        println(s"filtered Map ${filtered.size}")
        edgeList
      }
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
    val sRDD = sc.NetcdfDFSFile(hdfspath, List("ch4", "longitude", "latitude"), partCount)
//    val sRDD = sc.randomMatrices("/Users/sujen/Desktop/development/SciSpark/resources/testing/random.txt",
//      List("ch4"), (20,20), 2)
    val labeled = createLabelledRDD(sRDD)
    val collected = labeled.collect()
    val lon = collected(0).variables("longitude").data
    val lat = collected(0).variables("latitude").data
    val collected_filtered = collected.map(p => p(variable) <= 241.0)
//    val filtered = labeled.map(p => p(variable) <= 241.0)
    val consecFrames = collected_filtered.flatMap(p => {
      List((p.metaData("FRAME").toInt, p), (p.metaData("FRAME").toInt + 1, p))
    }).groupBy(_._1)
      .map(p => p._2.map(e => e._2).toList)
      .filter(p => p.size > 1)
      .map(p => p.sortBy(_.metaData("FRAME").toInt))
      .map(p => (p(0), p(1)))

    val MCCEdgeList = new mutable.MutableList[MCCEdge]
    val MCCNodeMap = new mutable.HashMap[String, Any]()

    val componentFrameRDD = consecFrames.flatMap({
      case (t1, t2) => {
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
        var overlappedPairsList = mutable.MutableList[((Double, Double))]()
        /**
          * The areaMinMaxTable keeps track of the area, minimum value, and maximum value
          * of all labeled regions in both components. For this reason the hash key has the following form :
          * 'F : C' where F = Frame Number and C = Component Number.
          * The areaMinMaxTable is updated by the updateComponent function, which is called in the for loop.
          *
          * @todo Extend it to have a lat long bounds for component.
          */
        var areaMinMaxTable = new mutable.HashMap[String, mutable.HashMap[String, Any]]

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
            var currentProperties = new mutable.HashMap[String, Double]()
            var metadata = new mutable.HashMap[String, Any]()
            if (areaMinMaxTable.contains(frame + ":" + label)) {
              metadata = areaMinMaxTable.get(frame + ":" + label).get
              currentProperties = metadata.get("properties").getOrElse().asInstanceOf[mutable.HashMap[String, Double]]
              grid = metadata.get("grid").getOrElse().asInstanceOf[mutable.HashMap[String, Double]]
              area = currentProperties.get("area").get
              max = currentProperties.get("maxTemp").get
              min = currentProperties.get("minTemp").get
              rowMax = currentProperties.get("rowMax").get
              colMax = currentProperties.get("colMax").get
              rowMin = currentProperties.get("rowMin").get
              colMin = currentProperties.get("colMin").get

              if (value < min) {
                min = value
                currentProperties += (("minTemp", min))
              }
              if (value > max) {
                max = value
                currentProperties += (("maxTemp", max))
              }

            } else {
              currentProperties += (("minTemp", value))
              currentProperties += (("maxTemp", value))
            }
            rowMax = if (row > rowMax) row else rowMax
            colMax = if (col > colMax) col else colMax
            rowMin = if (row < rowMin) row else rowMin
            colMin = if (col < colMin) col else colMin


            area += 1
            currentProperties += (("area", area))
            currentProperties += (("rowMax", rowMax))
            currentProperties += (("colMax", colMax))
            currentProperties += (("rowMin", rowMin))
            currentProperties += (("colMin", colMin))
            currentProperties += (("latMin", lat(rowMin.toInt)))
            currentProperties += (("latMax", lat(rowMax.toInt)))
            currentProperties += (("lonMin", lon(colMin.toInt)))
            currentProperties += (("lonMax", lon(colMax.toInt)))
            currentProperties += (("centerLat", ((lat(rowMax.toInt)+lat(rowMin.toInt))/2)))
            currentProperties += (("centerLon", ((lon(colMax.toInt)+lon(colMin.toInt))/2)))


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
        println(s"Overlap Map ${overlappedMap.size}")

        /**
          * Once the overlapped pairs have been computed, eliminate all duplicates
          * by converting the collection to a set. The component edges are then
          * mapped to the respective frames, so the global space of edges (outside of this task)
          * consists of unique tuples.
          */
        val edgesSet = overlappedPairsList.toSet
        println(s"Overlap Set ${edgesSet.size}  : ") // for debugging


        val edges = edgesSet.map({ case (c1, c2) => ((t1.metaData("FRAME"), c1), (t2.metaData("FRAME"), c2)) })
        println(s"Edges ${edges.size} ") // for debugging
        val filtered = edges.filter({
            case ((frameId1, compId1), (frameId2, compId2)) => {
              val frame_component1 = frameId1 + ":" + compId1
              val cloud1 = areaMinMaxTable(frame_component1)("properties").asInstanceOf[mutable.HashMap[String, Double]]
              val (area1, min1, max1) = (cloud1("area"), cloud1("minTemp"), cloud1("maxTemp"))
              val isCloud1 = ((area1 >= 2400.0) || ((area1 < 2400.0) && ((min1/max1) < 0.9)))
              val frame_component2 = frameId2 + ":" + compId2
              val cloud2 = areaMinMaxTable(frame_component2)("properties").asInstanceOf[mutable.HashMap[String, Double]]
              val (area2, max2, min2) = (cloud2("area"), cloud2("minTemp"), cloud2("maxTemp"))
              val isCloud2 = ((area2 >= 2400.0) || ((area2 < 2400.0) && ((min2/max2) < 0.9)))
              var meetsCriteria = true
              if(isCloud1 && isCloud2) {
                val areaOverlap = overlappedMap.get(compId1, compId2).get
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
                if(meetsCriteria) {
                  var node1: MCCNode = new MCCNode(frameId1.toInt, compId1.toFloat)
                  node1.setMetadata(areaMinMaxTable(frameId1 + ":" + compId1))

                  var node2: MCCNode = new MCCNode(frameId2.toInt, compId2.toFloat)
                  node2.setMetadata(areaMinMaxTable(frameId2 + ":" + compId2))

                  MCCNodeMap += (((frameId1 + ":" + compId1), node1))
                  MCCNodeMap += (((frameId2 + ":" + compId2), node2))
                  MCCEdgeList += new MCCEdge(node1, node2, edgeWeight)

                }
              }
              isCloud1 && isCloud2 && meetsCriteria
            }
          })

        val edgeList = new mutable.MutableList[((String, Double), (String, Double), Int, Any)]()
        filtered.foreach(edge => {
          val key = (edge._1._2, edge._2._2)
          if(overlappedMap.contains(key)) {
            val gridMap = areaMinMaxTable(edge._1._1 + ":" + edge._1._2)
            edgeList += ((edge._1, edge._2, overlappedMap.get(key).get, gridMap("grid")))
          }
        })

        //println(s"edgeList Map filetered ${edgeList.size}: $edgeList")
        //println(s"filtered Map ${filtered.size}")
        MCCEdgeList
      }
    })

//    val json = new JSONObject(MCCNodeMap.toMap)
    val pw = new PrintWriter("MCCNodesLines.json")
    MCCNodeMap.foreach{ case (key, value) => {
      pw.write(value.toString)
      pw.write("\n")
    }}
    pw.close()

    val fw = new FileWriter("MCCEdges.txt")
    fw.write(MCCEdgeList.toString())
    fw.close()


  }
}

