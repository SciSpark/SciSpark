import breeze.linalg.DenseMatrix
import org.dia._
import org.jblas.DoubleMatrix
/**
 * Created by rahulsp on 6/19/15.
 */
class Main$Test extends org.scalatest.FunSuite {
  test("BlockAvrgArrayTest") {
    val squareSize = 100
    val reductionSize = 50
    val accuracy = 1E-4
    val reducedWidth = squareSize / reductionSize
    val testMatrix = DoubleMatrix.ones(squareSize, squareSize)
    val resultMatrix = Main.jblasreduceResolution(testMatrix, reductionSize)

    for (i <- 0 to (reducedWidth - 1)) {
      for (j <- 0 to (reducedWidth - 1)) {
        val error = Math.abs(resultMatrix.get(i, j) - 1)
        if (error >= accuracy) {
          assert(error >= accuracy, "The error is not even close for indices " + i + " " + j + "with value : " + resultMatrix.get(i, j))
        }
      }
    }
  }

  /**
   * Sets the values in the first row to be NaN's
   * The average in the first element of the reduced Matrix should be
   * 49/50. If not, then NaN's were not properly accounted for.
   *
   * TODO :: This test needs to fail - not sure why it isn't failing
   */
    test("BlockAvrgArrayNanTest") {
      val squareSize = 100
      val reductionSize = 50
      val accuracy = 1E-15
      val reducedWidth = squareSize / reductionSize
      var testMatrix = DoubleMatrix.ones(squareSize, squareSize)
      for(i <- 0 to squareSize) {
        testMatrix = testMatrix.put(i, 0, Double.NaN)
        testMatrix = testMatrix.put(i, 1, Double.NaN)
        testMatrix = testMatrix.put(i, 2, Double.NaN)
      }

      val resultMatrix = Main.jblasreduceResolution(testMatrix, reductionSize)

      for(i <- 0 to (reducedWidth - 1)) {
        for (j <- 0 to (reducedWidth - 1)) {
          val error = Math.abs(resultMatrix.get(i, j) - 1)
          if (error >= accuracy) {
            assert(error >= accuracy, "The error is not even close for indices " + i + " " + j + "with value : " + resultMatrix.get(i, j))
          }
        }
      }
    assert(true)
  }

//  test("breezeReduceResolutionAvrgTest") {
//    val squareSize = 100
//    val reductionSize = 50
//    val accuracy = 1E-15
//    val reducedWidth = squareSize / reductionSize
//    val testMatrix = DenseMatrix.ones[Double](squareSize, squareSize)
//
//    val resultMatrix = Main.breezereduceResolution(testMatrix, reductionSize)
//
//    for(i <- 0 to (reducedWidth - 1)){
//      for(j <- 0 to (reducedWidth - 1)) {
//        val error = Math.abs(resultMatrix(i, j) - 1)
//        if(error >= accuracy) {
//          assert(error >= accuracy, "The error is not even close for indices " + i + " " + j + "with value : " + resultMatrix(i, j))
//        }
//      }
//    }
//    assert(true)
//  }
}
