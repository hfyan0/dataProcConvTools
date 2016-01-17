import org.scalatest.junit.AssertionsForJUnit
import scala.collection.mutable.ListBuffer
import org.junit.Assert._
import org.junit.Test
import org.junit.Before
import scala.util.Random

class DataConvertTest extends AssertionsForJUnit {

  @Before def initialize() {
    println("Initializing tests.")
  }

  @Test def testUpdateOHCL() {

    //--------------------------------------------------
    // testing with random numbers
    //--------------------------------------------------
    val r = new scala.util.Random(2357)
    val ohlc2 = new DataConvert.OHLCAcc()
    assertFalse(ohlc2.ready)
    for (j <- 0 to 10000) {
      val s = scala.collection.mutable.Set[Double]()
      var o = -1D
      var c = -1D
      var v = 0L
      ohlc2.reset
      for (i <- 0 to 50) {
        val rand_price = r.nextInt(1000).toDouble
        val rand_volume = r.nextInt(1000).toLong
        if (o < 0) o = rand_price
        c = rand_price
        ohlc2.updateOHLC(rand_price, rand_volume)
        s += rand_price
        v += rand_volume
      }
      assertEquals(ohlc2.getOHLCBar, new DataConvert.OHLCBar(o, s.max, s.min, c, v))
    }

  }

}
