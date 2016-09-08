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

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import org.dia.core.SciTensor
import org.dia.tensors.AbstractTensor



/**
 * Utilities to compute connected components within tensor.
 */
object MCCOps {

  val BACKGROUND = 0.0
  val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)
  /**
   * Reduces the resolution of the tensor into square blocks
   * by taking the average.
   *
   * @param tensor original tensor
   * @param blockSize block size used for aggregation
   * @param invalid value used to signify invalid value
   * @return aggregated tensor obtained by taking the average of
   * values within blocks ignoring invalid values
   */
  def reduceResolution(tensor: AbstractTensor, blockSize: Int, invalid: Double = Double.NaN): AbstractTensor = {
    val largeArray = tensor
    val numRows = largeArray.rows()
    val numCols = largeArray.cols()
    val reducedMatrix = tensor.zeros(numRows / blockSize, numCols / blockSize)

    for (row <- 0 until reducedMatrix.rows) {
      for (col <- 0 until reducedMatrix.cols) {
        val rowRange = (row * blockSize) -> ((row + 1) * blockSize)
        val columnRange = (col * blockSize) -> ((col + 1) * blockSize)
        val block = tensor(rowRange, columnRange).copy
        val numValid = block.data.count(_ != invalid)
        val avg = if (numValid > 0) block.cumsum / numValid else 0.0
        reducedMatrix.put(avg, row, col)
      }
    }
    reducedMatrix
  }

  /**
   * Reduces the resolution of the tensor into rectangle blocks
   * by taking the average.
   *
   * Similar to above method reduceResolution.
   */
  def reduceRectangleResolution(tensor: AbstractTensor,
                                rowSize: Int,
                                colSize: Int,
                                invalid: Double = Double.NaN): AbstractTensor = {
    val largeArray = tensor
    val numRows = largeArray.rows()
    val numCols = largeArray.cols()
    val reducedMatrix = tensor.zeros(numRows / rowSize, numCols / colSize)

    for (row <- 0 until reducedMatrix.rows) {
      for (col <- 0 until reducedMatrix.cols) {
        val rowRange = (row * rowSize) -> ((row + 1) * rowSize)
        val columnRange = (col * colSize) -> ((col + 1) * colSize)
        val block = tensor(rowRange, columnRange).copy
        val numValid = block.data.count(_ != invalid)
        val avg = if (numValid > 0) block.cumsum / numValid else 0.0
        reducedMatrix.put(avg, row, col)
      }
    }
    reducedMatrix
  }

  /**
   * Computes the cloud connected components.
   *
   * @param tensor the tensor
   * @return List of tensors that are the connected components.
   * Returned as a mask but with metadata such as number of component,
   * area, difference (= max - min).
   */
  def findCloudComponents(tensor: SciTensor): List[SciTensor] = {
    val labeledTensors = findConnectedComponents(tensor.tensor)
    val absT: AbstractTensor = tensor.tensor

    val comps = labeledTensors.indices.map(p => {
      val masked: AbstractTensor = labeledTensors(p).map(a => if (a != 0.0) 1.0 else a)
      val areaTuple = computeBasicStats(masked * absT)
      val area = areaTuple._1
      val max = areaTuple._2
      val min = areaTuple._3
      val metadata = tensor.metaData +=
        (("AREA", "" + area)) +=
        (("CONVECTIVE_FRACTION", "" + (min/max))) +=
        (("COMPONENT", "" + p))
      new SciTensor(tensor.varInUse, masked, metadata)
    })
    comps.toList
  }

  /**
   * Computes connected components of tensor.
   *
   * Note that each returned component is the entire tensor itself
   * just with the cells that do not fall into the component
   * zeroed out. The cells that do fall into the component all have
   * the number of the component as their value.
   *
   * @param tensor the tensor
   * @return masked connected components
   */
  def findConnectedComponents(tensor: AbstractTensor): List[AbstractTensor] = {
    val tuple = labelConnectedComponents(tensor)
    val labeled = tuple._1
    val maxVal = tuple._2
    val maskedLabels = (1 to maxVal).toArray.map(labeled := _.toDouble)
    maskedLabels.toList
  }

  /**
   * Computes connected component labels of tensor.
   *
   * Note that for garbage collection purposes we use one ArrayStack.
   * We push in row/col tuples two ints at a time, and pop two ints at a time.
   * Also note that the components are labeled starting at 1.
   *
   * This is just doing a depth-first-search (DFS) over the tensor cells.
   * (The tensor cells are a graph with two cells being connected iff
   * they are next to each other horizontally or vertically (but not diagonally).)
   *
   * @param tensor the tensor
   * @return a single tensor with the associated component numbers,
   * together with the total number of components.
   */
  def labelConnectedComponents(tensor: AbstractTensor): (AbstractTensor, Int) = {
    val fourVector = List((1, 0), (-1, 0), (0, 1), (0, -1))
    val rows = tensor.rows()
    val cols = tensor.cols()
    val labels = tensor.zeros(tensor.shape: _*)
    var label = 1
    val stack = new util.ArrayDeque[Int](tensor.rows + tensor.cols * 10)

    /**
     * Whether we already assigned a component label to the coordinate.
     *
     * (row,col) is already labeled if
     * (i) it's out of bounds OR
     * (ii) its value is BACKGROUND (=0) OR
     * (iii) it has already been assigned to a component,
     * i.e. labels(row,col) is not BACKGROUND (=0)
     *
     * @param row the row to check
     * @param col the column to check
     * @return whether (row,col) is already labeled
     */
    def isLabeled(row: Int, col: Int): Boolean = {
      if (row < 0 || col < 0 || row >= rows || col >= cols) return true
      tensor(row, col) == BACKGROUND || labels(row, col) != BACKGROUND
    }

    /**
     * Pushes unlabeled neighbors onto the stack.
     *
     * @param currentLabel the component label being added
     */
    def dfs(currentLabel: Int): Unit = {
      while (!stack.isEmpty) {
        /**
         *  Note that when popping, we pop col, then row.
         *  When pushing, we push row, then col.
         */
        val col = stack.pop
        val row = stack.pop
        labels.put(currentLabel, row, col)
        val neighbors = fourVector.map(p => (p._1 + row, p._2 + col))
        for (neighbor <- neighbors) {
          if (!isLabeled(neighbor._1, neighbor._2)) {
            val row = neighbor._1
            val col = neighbor._2
            stack.push(row)
            stack.push(col)
          }
        }
      }
    }

    /** Main loop */
    for (row <- 0 until rows) {
      for (col <- 0 until cols) {
        if (!isLabeled(row, col)) {
          stack.push(row)
          stack.push(col)
          dfs(label)
          label += 1
        }
      }
    }
    (labels, label - 1)
  }

  /**
   * Computes basic statistics about tensor.
   *
   * @param tensor the tensor
   * @return area, min, max
   */
  def computeBasicStats(tensor: AbstractTensor): (Double, Double, Double) = {
    var count = 0.0
    var min = Double.MaxValue
    var max = Double.MinValue
    val masked = tensor.map(p => {
      if (p != 0) {
        if (p < min) min = p
        if (p > max) max = p
        count += 1.0
        1.0
      } else {
        p
      }
    })
    (count, max, min)
  }

  /**
   * Very similar to labelConnectedComponents except also
   * adds number of components as meta information.
   * Further it is expecting and returning a SciTensor,
   * not an AbstractTensor.
   */
  def findCloudElements(tensor: SciTensor): SciTensor = {
    val labeledTensor = labelConnectedComponents(tensor.tensor)
    val metadata = tensor.metaData += (("NUM_COMPONENTS", "" + labeledTensor._2))
    new SciTensor(tensor.varInUse, labeledTensor._1, metadata)
  }

  /**
   * Checks whether connected component is indeed a cloud.
   *
   * @param comp masked component with AREA and CONVECTIVE_FRACTION meta info
   * @todo make sure this is only applied if AREA and CONVECTIVE_FRACTION
   * meta fields exist!
   */
  def checkCriteria(comp: SciTensor): Boolean = {
    val hash = comp.metaData
    val area = hash("AREA").toDouble
    val convectiveFrac = hash("CONVECTIVE_FRACTION").toDouble
    ((area >= 2400.0) || ((area < 2400.0) && ((convectiveFrac) > 0.9)))
  }

  /**
   * Returns the edges from first to second tensor.
   *
   * @param sciTensor1 first tensor
   * @param sciTensor2 second tensor
   * @return list of (String, String) = (frameId1:componentId1, frameId2:componentId2)
   * such that componentId1+2 satisfy the criteria and overlap.
   */
  def checkComponentsOverlap(sciTensor1: SciTensor, sciTensor2: SciTensor): List[(String, String)] = {
    val currentTimeRDD = MCCOps.findCloudElements(sciTensor1)
    val nextTimeRDD = MCCOps.findCloudElements(sciTensor2)
    var edges = List.empty[(String, String)]
    /** Cartesian product */
    (1 to currentTimeRDD.metaData("NUM_COMPONENTS").toInt).foreach(cIdx => {
      (1 to nextTimeRDD.metaData("NUM_COMPONENTS").toInt).foreach(nIdx => {
        /** check if valid */
        if (checkCriteria(sciTensor1.tensor.data, currentTimeRDD.tensor.data, cIdx)
          && checkCriteria(sciTensor2.tensor.data, nextTimeRDD.tensor.data, nIdx)) {
          /** verify overlap */
          if (overlap(currentTimeRDD.tensor, nextTimeRDD.tensor, cIdx, nIdx)) {
            val tup = (currentTimeRDD.metaData("FRAME") + ":" + cIdx, nextTimeRDD.metaData("FRAME") + ":" + nIdx)
            edges :+= tup
          }
        }
      })
    })
    edges
  }

  /**
   * Whether component satisfies criteria.
   *
   * @param origData linearized tensor values
   * @param compData linearized tensor component labels
   * @param compNum component label
   * @return whether component satisfies criteria
   * @todo make it only callable from within checkComponentsOverlap
   */
  def checkCriteria(origData: Array[Double], compData: Array[Double], compNum: Int): Boolean = {
    var area = 0.0
    var cnt = 0
    val maskedTen = compData.map(e => {
      cnt += 1
      if (e == compNum) {
        area += 1.0
        1.0
      } else 0.0
    })
    var dMax = Double.MinValue
    var dMin = Double.MaxValue
    var idx = 0
    /** @todo is there a better soln than initializing it to 0.0? */
    var curVal = 0.0
    while (idx < cnt) {
      if (maskedTen(idx) != 0) {
        curVal = origData(idx)
        if (dMax < curVal) dMax = origData(idx)
        if (dMin > curVal) dMin = origData(idx)
      }
      idx += 1
    }
    ((area >= 2400.0) || ((area < 2400.0) && ((dMin/dMax) > 0.9)))
  }

  /**
   * Whether two components overlap.
   *
   * @param comps1 first component tensor
   * @param comps2 second component tensor
   * @param compNum1 first component number
   * @param compNum2 second component number
   * @return whether the two components overlap
   * @todo this method only makes sense if the two tensors are component tensors,
   * i.e. the values are the component numbers not normal values. make sure that
   * this method is thus only callable with such tensors, e.g. make it only callable
   * from within checkComponentsOverlap.
   */
  def overlap(comps1: AbstractTensor, comps2: AbstractTensor, compNum1: Int, compNum2: Int): Boolean = {
    /** mask for specific component */
    val maskedComp1 = comps1.map(e => {
      if (e == compNum1) 1.0 else 0.0
    })
    val maskedComp2 = comps1.map(e => {
      if (e == compNum2) 1.0 else 0.0
    })
    /** check overlap */
    !(maskedComp1 * maskedComp2).isZeroShortcut
  }

  /**
   * Method to partition the edges into buckets containing a group of
   * consecutive nodes
   * @param edge MCCEdge
   * @param bucketSize Number of nodes to put in one bucket (size of partition)
   * @param partitionCount Number of partitions required
   * @return A bucket number (int) and edge
   */
  def mapEdgesToBuckets(edge: MCCEdge, bucketSize: Int, partitionCount: Int): (Int, MCCEdge) = {
    val edgePartitionKey: Int = edge.metadata("index").toInt
    for (i <- 1 to partitionCount) {
      if (edgePartitionKey <= bucketSize * i) {
        val bucket: Int = i
        return (bucket, edge)
      }
    }
    /** Place the edge in the last partition, if in none of the above */
    return (partitionCount, edge)
  }

  /**
   * @todo Come up with a way to find out border nodes when frame numbers are of type 2006091100
   * @param partition
   * @param currentIteration
   * @param bucketSize
   * @return
   */
  def processEdgePartition(partition: (Int, Iterable[MCCEdge]),
                           currentIteration: Int, minGraphLength: Int,
                           bucketSize: Int): (Int, Iterable[MCCEdge]) = {

    logger.info(s"Processing partition for key: ${partition._1} at iteration: $currentIteration" +
      s" with edges: ${partition._2}")

    /** The current max-size of the partition, since we recursively merge two paritions
     * we get the partition size by multiplying the # of iteration and individual partition size
     */
    val partitionMaxSize = bucketSize * math.pow(2, currentIteration -1).toInt
    val partitionIndex: Int = partition._1
    val partitionStartNodeIndex: Int = partitionMaxSize * (partitionIndex-1)
    val partitionEndNodeIndex: Int = partitionMaxSize * partitionIndex

    val firstEdge = partition._2.toSeq(0)
    val partitionStartFrameNum =
      if (firstEdge.metadata("index").toInt == partitionStartNodeIndex) firstEdge.srcNode.frameNum else -1

    val lastEdge = partition._2.toSeq(partition._2.size - 1)
    val partitionEndFrameNum =
      if (lastEdge.metadata("index").toInt == partitionEndNodeIndex) lastEdge.srcNode.frameNum else -1

    /** To have a key and a set of values, we use MultiMap */
    val edgeMap = new mutable.HashMap[String, MCCEdge]
    val edgeMapString = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
    val nodeMap = new mutable.HashMap[String, MCCNode]()
    /** A set of nodes who have 0 incoming edges */
    val originNodes = mutable.HashSet[MCCNode]()

    for(edge <- partition._2) {
      val srcKey = edge.srcNode.hashKey()
      val destKey = edge.destNode.hashKey()
      edgeMap(edge.hashKey()) = edge
      edgeMapString.addBinding(srcKey, edge.hashKey())
    }

    /** Edges we need to carry forward to the next iteration */
    val filteredEdges = new mutable.HashSet[String]()
    val subgraphList = new mutable.HashSet[String]()
    val discardedEdges = new mutable.HashSet[String]()

    for(node <- edgeMapString.keys) {
      val graphInfo = getGraphInfo(node, 0, false, edgeMapString, new mutable.HashSet[String](),
        partitionEndFrameNum.toString, partitionStartFrameNum.toString)

      /** If the graph has a border node then we store the entire graph containing it for the next iteration */
      if (graphInfo._3) {
        filteredEdges ++= graphInfo._2
      }
      else {
        if (graphInfo._1 >= minGraphLength) {
          subgraphList ++= graphInfo._2
        }
        else {
          discardedEdges ++= graphInfo._2
          logger.info(s"Iteration $currentIteration," +
            s"PartitionIndex: $partitionIndex," +
            s"Discarded Edges : ${graphInfo._2}")
        }
      }
    }
    logger.info(s"Iteration $currentIteration," +
      s"PartitionIndex: $partitionIndex," +
      s"Subgraph found : ${subgraphList.toSeq.sorted}")

    if (filteredEdges.isEmpty) {
      logger.info(s"Iteration $currentIteration," +
        s"PartitionIndex: $partitionIndex," +
        s"No edges in FilteredEdges found")
//      return (-1, filteredEdges).
      return (-1, new mutable.MutableList[MCCEdge]())
    }
    val newIndex = if (partitionIndex%2==0) partitionIndex/2 else (1 + partitionIndex/2)
    logger.info(s"Sending to new partition, iteration : $currentIteration, edges: $filteredEdges")

    val returnedEdges = edgeMap.filter(x => {
      filteredEdges.contains(x._1)
    })
    return (newIndex, returnedEdges.values)
  }

  /**
   * To get the max length of the subgraph, starting from the
   * source node to the farthest child.
   * Also return a boolean if the subgraph contains a border edge.
   * If it contains a border that means we need further investigation.
   * @param srcNode Root node to start traversal
   * @param length Length of the graph
   * @param borderNodeFlag To check if the graph contains a border node
   * @param edgeMap All the edges in the subgraph originating from the graph traversal
   *                 from the given srcNode
   * @return Tuple (length of graph, all edges in the graph, boolean value if the
   *         graph contains a border node)
   */
  def getGraphInfo(srcNode: String, length: Int, borderNodeFlag: Boolean,
                   edgeMap: mutable.HashMap[String, mutable.Set[String]],
                   edgeList: mutable.HashSet[String],
                   endFrameNum: String, startFrame: String):
  (Int, mutable.HashSet[String], Boolean) = {
    var maxLength = length
    var hasBorderNode = borderNodeFlag
    if (edgeMap.contains(srcNode)) {
      for (outEdge <- edgeMap(srcNode)) {
        edgeList += outEdge
        hasBorderNode |= (srcNode.split(":")(0) == startFrame || srcNode.split(":")(0) == endFrameNum)
        val graphInfo = getGraphInfo(outEdge.split(",")(1), length + 1,
          hasBorderNode, edgeMap, edgeList, endFrameNum, startFrame)
        maxLength = if (maxLength < graphInfo._1) graphInfo._1 else maxLength
        hasBorderNode |= graphInfo._3
      }
    }
    return (maxLength, edgeList, hasBorderNode)
  }

  /**
   * Method to recursively generate subgraphs from the partitions
   * @param edgeList
   * @return Array[(Int, Iterable[MCCEdge])]
   */
  def findSubgraphsIteratively(edgeList: RDD[(Int, Iterable[MCCEdge])], iteration: Int,
                               buckerSize: Int,
                               minGraphLength: Int,
                               sc: SparkContext): Array[(Int, Iterable[MCCEdge])] = {
    var iter = iteration
    def startProcessing(obj: RDD[(Int, Iterable[MCCEdge])]): RDD[(Int, Iterable[MCCEdge])] = {
      obj.map(x => processEdgePartition(x, iter, minGraphLength, buckerSize))
        .filter(x => x._1 != -1)
        .reduceByKey((x, y) => {
          val merged = new mutable.HashSet[MCCEdge]()
          merged ++= x
          merged ++= y
          merged
        })
    }

    var newGraph = startProcessing(edgeList)
    iter += 1
    /** if edgeList is empty implies that all valid subgraphs were found */
    while (newGraph.count() > 1) {
        val temp = startProcessing(newGraph)
      newGraph = temp
      iter += 1
      logger.debug(edgeList.toDebugString)
    }
    val temp = startProcessing(newGraph).collect()
    return temp
  }

  /**
   *
   * @param edgeListRDD
   */
  def createPartitionIndex(edgeListRDD: RDD[MCCEdge]): RDD[MCCEdge] = {
    val temp = edgeListRDD.map(edge => (edge.srcNode.frameNum, List(edge)))
      .reduceByKey(_ ++ _)
      .sortBy(_._1)
      .zipWithIndex

    temp.flatMap({
      case ((key, edges), index) =>
        edges.map(edge => edge.updateMetadata("index", index.toString))
    })
  }
}
