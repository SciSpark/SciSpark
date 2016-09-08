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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag

import ucar.ma2.{ArrayDouble, ArrayInt, DataType}
import ucar.nc2.{Attribute, Dimension, NetcdfFileWriter, Variable}

import org.dia.core.SciTensor
import org.dia.tensors.AbstractTensor

object MCSUtils {

    val logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

    /**
     * Writes the node to a netCDF file
     *
     * @param filename String The full path and name of the netcdfFile.
     * @param varData ucar.ma2.ArrayInt.D2  2D data to be written to the file.
     * @param lats Array[Double] of the latitudes to be used
     * @param lons Array[Double] of the longitudes to be used
     */
    def netcdf_write(filename: String, varData: ucar.ma2.ArrayInt.D2, lats: Array[Double],
      lons: Array[Double]): Unit = {
        try {
            val fsplit = filename.split("_")
            val latMin = fsplit(1).toInt
            val latMax = fsplit(2).toInt
            val lonMin = fsplit(3).toInt
            val lonMax = fsplit(4).dropRight(3).toInt
            val lats1 = lats.slice(latMin, latMax + 1)
            val lons1 = lons.slice(lonMin, lonMax + 1)
            val datafile = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, filename, null)
            // Create netCDF dimensions
            val lonDim = datafile.addDimension(null, "longitudes", lons1.length)
            val latDim = datafile.addDimension(null, "latitudes", lats1.length)
            // create dim
            val dims = new util.ArrayList[Dimension]()
            val latDims = new util.ArrayList[Dimension]()
            val lonDims = new util.ArrayList[Dimension]()
            // add dims
            dims.add(latDim)
            dims.add(lonDim)
            latDims.add(latDim)
            lonDims.add(lonDim)
            // add data
            var rr = datafile.addVariable(null, "CE brightness temperature", DataType.INT, dims)
            var latDimVar = datafile.addVariable(null, "lat", DataType.DOUBLE, latDims)
            var lonDimVar = datafile.addVariable(null, "lon", DataType.DOUBLE, lonDims)
            // add attributes
            rr.addAttribute(new Attribute("units", "K"))
            // add dim data
            val latData = new ArrayDouble.D1(lats1.length)
            val lonData = new ArrayDouble.D1(lons1.length)
            for (i <- 0 until lats1.length) { latData.set(i, lats1(i))}
            for (i <- 0 until lons1.length) { lonData.set(i, lons1(i))}
            // create the file
            datafile.create()
            datafile.write(rr, varData)
            datafile.write(latDimVar, latData)
            datafile.write(lonDimVar, lonData)
            datafile.close()
        }
        catch {
            case _: Throwable => logger.info("Error generating netCDF file for " + filename + "\n")
        }
    }


    /**
     * Get the data from the nodes in the edge
     * @param edge Current MCCEdge
     * @param MCSNodeMap mutable.HashMap[String, MCCNode] representing the map of each node metadata
     * @param lats Array[Double] representing the latitudes
     * @param lons Array[Double] representing the longitudes
     * @param tightestBox Boolean to use tightest box around data
     */
    def get_node_data(edge: MCCEdge, MCSNodeMap: mutable.HashMap[String, MCCNode],
                      lats: Array[Double], lons: Array[Double], tightestBox: Boolean): Unit = {
        val srcNodeKey = edge.srcNode.hashKey()
        val srcNode = MCSNodeMap(srcNodeKey)
        extract_masked_data(srcNode, lats, lons, tightestBox)

        val destNodeKey = edge.destNode.hashKey()
        val destNode = MCSNodeMap(destNodeKey)
        extract_masked_data(destNode, lats, lons, tightestBox)
    }


    /** Extract the node mask from the MCCNode metadata
     * @param thisNode MCCNode the current node
     * @param lats Array[Double] representing the latitudes
     * @param lons Array[Double] representing the longitudes
     * @param tightestBox Boolean to use tightest box around data.
     */
    def extract_masked_data(thisNode: MCCNode, lats: Array[Double], lons: Array[Double], tightestBox: Boolean): Unit = {
        var nodeID = ""
        var latMin = .0
        var latMax = .0
        var lonMin = .0
        var lonMax = .0
        var lonMinOffset = 0
        var lonMaxOffset = 0
        var latMinOffset = 0
        var latMaxOffset = 0
        if (tightestBox == true) {
            latMin = lats.minBy(v => math.abs( v - (thisNode.getLatMin())))
            latMax = lats.minBy(v => math.abs( v - (thisNode.getLatMax())))
            lonMin = lons.minBy(v => math.abs( v - (thisNode.getLonMin())))
            lonMax = lons.minBy(v => math.abs( v - (thisNode.getLonMax())))
        }
        else {
            latMin = lats(0)
            latMax = lats.last
            lonMin = lons(0)
            lonMax = lons.last
        }
        latMinOffset = lats.indexOf(latMin)
        latMaxOffset = lats.indexOf(latMax)
        lonMinOffset = lons.indexOf(lonMin)
        lonMaxOffset = lons.indexOf(lonMax)
        var nodeGrid = new ArrayInt.D2(((latMaxOffset - latMinOffset) + 1), ((lonMaxOffset - lonMinOffset) + 1))
        var ima = nodeGrid.getIndex()
        val gridMap: mutable.HashMap[String, Double] = thisNode.grid
        gridMap.foreach{case(k, v) =>
            val indices = k.replace("(", "").replace(")", "").replace(" ", "").split(",")
            nodeGrid.setDouble(ima.set( (indices(0).toInt) - latMinOffset, (indices(1).toInt) - lonMinOffset), v.toInt)
        }
        nodeID = "/tmp/F" + thisNode.getFrameNum.toString + "CE" + thisNode.getCloudElemNum.toString.dropRight(2) +
          "_" + latMinOffset.toString + "_" + latMaxOffset.toString + "_" + lonMinOffset.toString + "_" +
          lonMaxOffset.toString + ".nc"
        netcdf_write(nodeID, nodeGrid, lats, lons)
    }
}
