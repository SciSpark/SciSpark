package org.dia.core

import java.util

import org.nd4j.linalg.api.buffer.DataBuffer
import org.nd4j.linalg.api.complex.{IComplexNDArray, IComplexNumber}
import org.nd4j.linalg.api.ndarray.INDArray
import org.nd4j.linalg.indexing.NDArrayIndex
import org.nd4j.linalg.indexing.conditions.Condition

/**
 * Created by marroqui on 7/15/15.
 */
class DataObject extends INDArray {

  val metaData: Map[String,String]
  val nDArray : INDArray

  override def div(number: Number): INDArray = ???

  override def div(number: Number, indArray: INDArray): INDArray = ???

  override def div(indArray: INDArray): INDArray = ???

  override def div(indArray: INDArray, indArray1: INDArray): INDArray = ???

  override def div(iComplexNumber: IComplexNumber): IComplexNDArray = ???

  override def div(iComplexNumber: IComplexNumber, iComplexNDArray: IComplexNDArray): IComplexNDArray = ???

  override def rdiviColumnVector(indArray: INDArray): INDArray = ???

  override def rank(): Int = ???

  override def negi(): INDArray = ???

  override def linearView(): INDArray = ???

  override def rsubi(number: Number): INDArray = ???

  override def rsubi(number: Number, indArray: INDArray): INDArray = ???

  override def rsubi(indArray: INDArray): INDArray = ???

  override def rsubi(indArray: INDArray, indArray1: INDArray): INDArray = ???

  override def rsubi(iComplexNumber: IComplexNumber): IComplexNDArray = ???

  override def rsubi(iComplexNumber: IComplexNumber, iComplexNDArray: IComplexNDArray): IComplexNDArray = ???

  override def addi(number: Number): INDArray = ???

  override def addi(number: Number, indArray: INDArray): INDArray = ???

  override def addi(indArray: INDArray): INDArray = ???

  override def addi(indArray: INDArray, indArray1: INDArray): INDArray = ???

  override def addi(iComplexNumber: IComplexNumber): IComplexNDArray = ???

  override def addi(iComplexNumber: IComplexNumber, iComplexNDArray: IComplexNDArray): IComplexNDArray = ???

  override def eq(number: Number): INDArray = ???

  override def eq(indArray: INDArray): INDArray = ???

  override def mul(number: Number): INDArray = ???

  override def mul(number: Number, indArray: INDArray): INDArray = ???

  override def mul(indArray: INDArray): INDArray = ???

  override def mul(indArray: INDArray, indArray1: INDArray): INDArray = ???

  override def mul(iComplexNumber: IComplexNumber): IComplexNDArray = ???

  override def mul(iComplexNumber: IComplexNumber, iComplexNDArray: IComplexNDArray): IComplexNDArray = ???

  override def mean(i: Int): INDArray = ???

  override def cleanup(): Unit = ???

  override def linearIndex(i: Int): Int = ???

  override def setStride(ints: Array[Int]): Unit = ???

  override def diviColumnVector(indArray: INDArray): INDArray = ???

  override def subColumnVector(indArray: INDArray): INDArray = ???

  override def putScalar(i: Int, v: Double): INDArray = ???

  override def putScalar(i: Int, v: Float): INDArray = ???

  override def putScalar(i: Int, i1: Int): INDArray = ???

  override def putScalar(ints: Array[Int], v: Double): INDArray = ???

  override def putScalar(ints: Array[Int], v: Float): INDArray = ???

  override def putScalar(ints: Array[Int], i: Int): INDArray = ???

  override def vectorAlongDimension(i: Int, i1: Int): INDArray = ???

  override def neg(): INDArray = ???

  override def diviRowVector(indArray: INDArray): INDArray = ???

  override def addColumnVector(indArray: INDArray): INDArray = ???

  override def getRows(ints: Array[Int]): INDArray = ???

  override def addiRowVector(indArray: INDArray): INDArray = ???

  override def rsubRowVector(indArray: INDArray): INDArray = ???

  override def eps(number: Number): INDArray = ???

  override def eps(indArray: INDArray): INDArray = ???

  override def getScalar(i: Int, i1: Int): INDArray = ???

  override def getScalar(i: Int): INDArray = ???

  override def getScalar(ints: Array[Int]): INDArray = ???

  override def `var`(i: Int): INDArray = ???

  override def slices(): Int = ???

  override def linearViewColumnOrder(): INDArray = ???

  override def putRow(i: Int, indArray: INDArray): INDArray = ???

  override def majorStride(): Int = ???

  override def norm2(i: Int): INDArray = ???

  override def cond(condition: Condition): INDArray = ???

  override def get(ndArrayIndexes: NDArrayIndex*): INDArray = ???

  override def eqi(number: Number): INDArray = ???

  override def eqi(indArray: INDArray): INDArray = ???

  override def rdivi(number: Number): INDArray = ???

  override def rdivi(number: Number, indArray: INDArray): INDArray = ???

  override def rdivi(indArray: INDArray): INDArray = ???

  override def rdivi(indArray: INDArray, indArray1: INDArray): INDArray = ???

  override def rdivi(iComplexNumber: IComplexNumber): IComplexNDArray = ???

  override def rdivi(iComplexNumber: IComplexNumber, iComplexNDArray: IComplexNDArray): IComplexNDArray = ???

  override def divRowVector(indArray: INDArray): INDArray = ???

  override def checkDimensions(indArray: INDArray): Unit = ???

  override def columns(): Int = ???

  override def getDouble(ints: Int*): Double = ???

  override def getDouble(i: Int): Double = ???

  override def getDouble(i: Int, i1: Int): Double = ???

  override def addiColumnVector(indArray: INDArray): INDArray = ???

  override def rdiv(number: Number): INDArray = ???

  override def rdiv(number: Number, indArray: INDArray): INDArray = ???

  override def rdiv(indArray: INDArray): INDArray = ???

  override def rdiv(indArray: INDArray, indArray1: INDArray): INDArray = ???

  override def rdiv(iComplexNumber: IComplexNumber): IComplexNDArray = ???

  override def rdiv(iComplexNumber: IComplexNumber, iComplexNDArray: IComplexNDArray): IComplexNDArray = ???

  override def isCleanedUp: Boolean = ???

  override def transposei(): INDArray = ???

  override def divi(number: Number): INDArray = ???

  override def divi(number: Number, indArray: INDArray): INDArray = ???

  override def divi(indArray: INDArray): INDArray = ???

  override def divi(indArray: INDArray, indArray1: INDArray): INDArray = ???

  override def divi(iComplexNumber: IComplexNumber): IComplexNDArray = ???

  override def divi(iComplexNumber: IComplexNumber, iComplexNDArray: IComplexNDArray): IComplexNDArray = ???

  override def max(i: Int): INDArray = ???

  override def muli(number: Number): INDArray = ???

  override def muli(number: Number, indArray: INDArray): INDArray = ???

  override def muli(indArray: INDArray): INDArray = ???

  override def muli(indArray: INDArray, indArray1: INDArray): INDArray = ???

  override def muli(iComplexNumber: IComplexNumber): IComplexNDArray = ???

  override def muli(iComplexNumber: IComplexNumber, iComplexNDArray: IComplexNDArray): IComplexNDArray = ???

  override def muliRowVector(indArray: INDArray): INDArray = ???

  override def resetLinearView(): Unit = ???

  override def cumsum(i: Int): INDArray = ???

  override def neqi(number: Number): INDArray = ???

  override def neqi(indArray: INDArray): INDArray = ???

  override def elementStride(): Int = ???

  override def put(ndArrayIndexes: Array[NDArrayIndex], indArray: INDArray): INDArray = ???

  override def put(ndArrayIndexes: Array[NDArrayIndex], number: Number): INDArray = ???

  override def put(ints: Array[Int], indArray: INDArray): INDArray = ???

  override def put(i: Int, i1: Int, indArray: INDArray): INDArray = ???

  override def put(i: Int, i1: Int, number: Number): INDArray = ???

  override def put(i: Int, indArray: INDArray): INDArray = ???

  override def putSlice(i: Int, indArray: INDArray): INDArray = ???

  override def prod(i: Int): INDArray = ???

  override def data(): DataBuffer = ???

  override def rows(): Int = ???

  override def gt(number: Number): INDArray = ???

  override def gt(indArray: INDArray): INDArray = ???

  override def length(): Int = ???

  override def addRowVector(indArray: INDArray): INDArray = ???

  override def sub(number: Number): INDArray = ???

  override def sub(number: Number, indArray: INDArray): INDArray = ???

  override def sub(indArray: INDArray): INDArray = ???

  override def sub(indArray: INDArray, indArray1: INDArray): INDArray = ???

  override def sub(iComplexNumber: IComplexNumber): IComplexNDArray = ???

  override def sub(iComplexNumber: IComplexNumber, iComplexNDArray: IComplexNDArray): IComplexNDArray = ???

  override def subArray(ints: Array[Int], ints1: Array[Int], ints2: Array[Int]): INDArray = ???

  override def endsForSlices(): Array[Int] = ???

  override def repmat(ints: Int*): INDArray = ???

  override def dimShuffle(objects: Array[AnyRef], ints: Array[Int], booleans: Array[Boolean]): INDArray = ???

  override def ordering(): Char = ???

  override def reshape(ints: Int*): INDArray = ???

  override def reshape(i: Int, i1: Int): INDArray = ???

  override def isColumnVector: Boolean = ???

  override def gti(number: Number): INDArray = ???

  override def gti(indArray: INDArray): INDArray = ???

  override def rsub(number: Number): INDArray = ???

  override def rsub(number: Number, indArray: INDArray): INDArray = ???

  override def rsub(indArray: INDArray, indArray1: INDArray): INDArray = ???

  override def rsub(indArray: INDArray): INDArray = ???

  override def rsub(iComplexNumber: IComplexNumber): IComplexNDArray = ???

  override def rsub(iComplexNumber: IComplexNumber, iComplexNDArray: IComplexNDArray): IComplexNDArray = ???

  override def size(i: Int): Int = ???

  override def getFloat(ints: Array[Int]): Float = ???

  override def getFloat(i: Int): Float = ???

  override def getFloat(i: Int, i1: Int): Float = ???

  override def getRow(i: Int): INDArray = ???

  override def transpose(): INDArray = ???

  override def mulRowVector(indArray: INDArray): INDArray = ???

  override def epsi(number: Number): INDArray = ???

  override def epsi(indArray: INDArray): INDArray = ???

  override def isMatrix: Boolean = ???

  override def rsubiRowVector(indArray: INDArray): INDArray = ???

  override def swapAxes(i: Int, i1: Int): INDArray = ???

  override def muliColumnVector(indArray: INDArray): INDArray = ???

  override def cumsumi(i: Int): INDArray = ???

  override def lt(number: Number): INDArray = ???

  override def lt(indArray: INDArray): INDArray = ???

  override def ravel(): INDArray = ???

  override def squaredDistance(indArray: INDArray): Double = ???

  override def setData(dataBuffer: DataBuffer): Unit = ???

  override def setData(floats: Array[Float]): Unit = ???

  override def mulColumnVector(indArray: INDArray): INDArray = ???

  override def distance2(indArray: INDArray): Double = ???

  override def min(i: Int): INDArray = ???

  override def rdivColumnVector(indArray: INDArray): INDArray = ???

  override def broadcast(ints: Int*): INDArray = ???

  override def mmul(indArray: INDArray): INDArray = ???

  override def mmul(indArray: INDArray, indArray1: INDArray): INDArray = ???

  override def divColumnVector(indArray: INDArray): INDArray = ???

  override def norm1(i: Int): INDArray = ???

  override def assign(indArray: INDArray): INDArray = ???

  override def assign(number: Number): INDArray = ???

  override def offset(): Int = ???

  override def sum(i: Int): INDArray = ???

  override def secondaryStride(): Int = ???

  override def lti(number: Number): INDArray = ???

  override def lti(indArray: INDArray): INDArray = ???

  override def subRowVector(indArray: INDArray): INDArray = ???

  override def rsubiColumnVector(indArray: INDArray): INDArray = ???

  override def rdivRowVector(indArray: INDArray): INDArray = ???

  override def normmax(i: Int): INDArray = ???

  override def getColumn(i: Int): INDArray = ???

  override def index(i: Int, i1: Int): Int = ???

  override def isScalar: Boolean = ???

  override def condi(condition: Condition): INDArray = ???

  override def subiRowVector(indArray: INDArray): INDArray = ???

  override def slice(i: Int, i1: Int): INDArray = ???

  override def slice(i: Int): INDArray = ???

  override def isVector: Boolean = ???

  override def putColumn(i: Int, indArray: INDArray): INDArray = ???

  override def element(): AnyRef = ???

  override def stride(i: Int): Int = ???

  override def stride(): Array[Int] = ???

  override def dup(): INDArray = ???

  override def vectorsAlongDimension(i: Int): Int = ???

  override def shape(): Array[Int] = ???

  override def neq(number: Number): INDArray = ???

  override def neq(indArray: INDArray): INDArray = ???

  override def subi(number: Number): INDArray = ???

  override def subi(number: Number, indArray: INDArray): INDArray = ???

  override def subi(indArray: INDArray): INDArray = ???

  override def subi(indArray: INDArray, indArray1: INDArray): INDArray = ???

  override def subi(iComplexNumber: IComplexNumber): IComplexNDArray = ???

  override def subi(iComplexNumber: IComplexNumber, iComplexNDArray: IComplexNDArray): IComplexNDArray = ???

  override def sliceVectors(list: util.List[INDArray]): Unit = ???

  override def getInt(ints: Int*): Int = ???

  override def add(number: Number): INDArray = ???

  override def add(number: Number, indArray: INDArray): INDArray = ???

  override def add(indArray: INDArray): INDArray = ???

  override def add(indArray: INDArray, indArray1: INDArray): INDArray = ???

  override def add(iComplexNumber: IComplexNumber): IComplexNDArray = ???

  override def add(iComplexNumber: IComplexNumber, iComplexNDArray: IComplexNDArray): IComplexNDArray = ???

  override def isRowVector: Boolean = ???

  override def getColumns(ints: Array[Int]): INDArray = ???

  override def rsubColumnVector(indArray: INDArray): INDArray = ???

  override def rdiviRowVector(indArray: INDArray): INDArray = ???

  override def isSquare: Boolean = ???

  override def permute(ints: Int*): INDArray = ???

  override def distance1(indArray: INDArray): Double = ???

  override def subiColumnVector(indArray: INDArray): INDArray = ???

  override def mmuli(indArray: INDArray): INDArray = ???

  override def mmuli(indArray: INDArray, indArray1: INDArray): INDArray = ???

  override def std(i: Int): INDArray = ???
}
