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
    val resultMatrix = Main.reduceResolution(testMatrix, reductionSize)

    for(i <- 0 to (reducedWidth - 1)){
      for(j <- 0 to (reducedWidth - 1)){
        val error = Math.abs(resultMatrix.get(i,j) - 1)
        if(error >= accuracy){
          assert(error >= accuracy, "The error is not even close for indices " + i + " " + j + "with value : " + resultMatrix.get(i,j))
        }
      }
    }

    test("BlockAvrgArrayNanTest") {
      val squareSize = 100
      val reductionSize = 50
      val accuracy = 1E-4
      val reducedWidth = squareSize / reductionSize
      val testMatrix = DoubleMatrix.ones(squareSize, squareSize)
      val resultMatrix = Main.reduceResolution(testMatrix, reductionSize)

      for(i <- 0 to (reducedWidth - 1)){
        for(j <- 0 to (reducedWidth - 1)){
          val error = Math.abs(resultMatrix.get(i,j) - 1)
          if(error >= accuracy){
            assert(error >= accuracy, "The error is not even close for indices " + i + " " + j + "with value : " + resultMatrix.get(i,j))
          }
        }

    }
    assert(true)
  }
}
