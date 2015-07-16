package org.dia.core

import breeze.linalg.DenseMatrix
import org.dia.TRMMUtils.Constants._
import org.dia.TRMMUtils.NetCDFUtils
import org.slf4j.Logger
import ucar.ma2
import ucar.nc2.dataset.NetcdfDataset

import scala.collection.mutable

/**
 * Created by rahulsp on 7/15/15.
 */
 trait ArrayLib[T] {
  val name : String
  var array : T
  // Class logger
  val LOG : Logger = org.slf4j.LoggerFactory.getLogger(this.getClass)

  /**
   * Loads in data given that it is a TRMM Dataset
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  def loadNetCDFTRMMVars (url : String, variable : String) : T

  /**
   * Gets an NDimensional array from a NetCDF file
   * @param url where the netcdf file is located
   * @param variable the NetCDF variable to search for
   * @return
   */
  def loadNetCDFNDVars (url : String, variable : String) : T

  /**
   * Reduces the resolution of a DenseMatrix
   * @param largeArray the array that will be reduced
   * @param blockSize the size of n x n size of blocks.
   * @return
   */
  def reduceResolution(largeArray: T, blockSize: Int): ArrayLib[T]

  /**
   * Creates a 2D array from a list of dimensions using a variable
   * @param dimensionSizes hashmap of (dimension, size) pairs
   * @param netcdfFile the NetcdfDataset to read
   * @param variable the variable array to extract
   * @return DenseMatrix
   */
  def create2dArray(dimensionSizes: mutable.HashMap[Int, Int], netcdfFile: NetcdfDataset, variable: String): T

  implicit def +(array : T) : T
}
