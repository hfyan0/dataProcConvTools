import java.io._
import org.joda.time.{Period, DateTime, Duration}

object DataConvert {
  case class OHLCPriceBar(val o: Double, val h: Double, val l: Double, val c: Double, val v: Long)
  case class OHLCBar(val dt: DateTime, val symbol: String, val priceBar: OHLCPriceBar) {

    override def toString: String = {
      var b = new StringBuilder
      b.append(symbol)
      b.append(",")
      b.append(dt.getYear().toString)
      b.append("-")
      b.append(String.format("%2s", dt.getMonthOfYear().toString).replace(' ', '0'))
      b.append("-")
      b.append(String.format("%2s", dt.getDayOfMonth().toString).replace(' ', '0'))
      b.append(",")
      b.append(String.format("%2s", dt.getHourOfDay().toString).replace(' ', '0'))
      b.append(":")
      b.append(String.format("%2s", dt.getMinuteOfHour().toString).replace(' ', '0'))
      b.append(":")
      b.append(String.format("%2s", dt.getSecondOfMinute().toString).replace(' ', '0'))
      b.append(",")
      b.append(priceBar.o.toString)
      b.append(",")
      b.append(priceBar.h.toString)
      b.append(",")
      b.append(priceBar.l.toString)
      b.append(",")
      b.append(priceBar.c.toString)
      b.append(",")
      b.append(priceBar.v.toString)

      b.toString
    }

  }
  case class TradeTick(val dt: DateTime, val symbol: String, val price: Double, val volume: Long)

  def getEpoch: DateTime = new DateTime(1970, 1, 1, 0, 0, 0)

  def parseHKExFmt1(s: String): TradeTick = {
    // 08002  8003630   5.290      11000U09200020140102 0
    // 08002  8003630   5.290       5000U09200020140102 0
    // 08002  8003630   5.290       4000U09200020140102 0

    val symbol = s.substring(0, 5)
    val price = s.substring(14, 22).trim.toDouble
    val volume = s.substring(22, 33).trim.toLong

    val year = s.substring(40, 44)
    val month = s.substring(44, 46)
    val day = s.substring(46, 48)
    val hour = s.substring(34, 36)
    val minute = s.substring(36, 38)
    val sec = s.substring(38, 40)
    val dt = new DateTime(year.toInt, month.toInt, day.toInt, hour.toInt, minute.toInt, sec.toInt)

    new TradeTick(dt, symbol, price, volume)

    // pw.write(symbol.toString)
    // pw.write(",")
    // pw.write(price.toString)
    // pw.write(",")
    // pw.write(volume.toString)
    // pw.write(",")
    // pw.write(dt.toString)
    // pw.write("\n")
  }

  class OHLCAcc {
    var _o: Double = -1
    var _h: Double = -1
    var _l: Double = -1
    var _c: Double = -1
    var _v: Long = 0

    def updateOHLC(tickPrice: Double, volume: Long) {
      if (_o < 0) {
        _o = tickPrice
        _h = tickPrice
        _l = tickPrice
        _c = tickPrice
        _v = volume
      }
      else {
        if (tickPrice > _h) _h = tickPrice
        if (tickPrice < _l) _l = tickPrice
        _c = tickPrice
        _v += volume
      }
      Unit
    }
    def getOHLCPriceBar: OHLCPriceBar = new OHLCPriceBar(_o, _h, _l, _c, _v)

    def reset {
      _o = _c
      _h = _c
      _l = _c
      _v = 0
    }
    def ready: Boolean = {
      if (_o < 0) false
      else true
    }
  }

  class OHLCOutputAdaptor(val startTime: DateTime, val endTime: DateTime, val barIntervalInSec: Int) {

    var ohlcacc = new OHLCAcc()
    var prev_bar_dt = startTime
    var prev_bar_sym = ""

    def convertToOHLCOutput(tradetick: TradeTick): List[String] = {

      var outList = scala.collection.mutable.ListBuffer[String]()

      if (prev_bar_sym != tradetick.symbol && prev_bar_sym != "") {
        //--------------------------------------------------
        // finish off the previous symbol first
        // illiquid stocks may not have trades just before market close
        //--------------------------------------------------
        while (prev_bar_dt.getSecondOfDay < endTime.getSecondOfDay) {
          prev_bar_dt = prev_bar_dt.plusSeconds(barIntervalInSec)
          val curPriceBar = ohlcacc.getOHLCPriceBar
          val curBar = new OHLCBar(prev_bar_dt, prev_bar_sym, curPriceBar)
          if (ohlcacc.ready &&
            prev_bar_dt.getSecondOfDay <= endTime.getSecondOfDay) {
            outList += curBar.toString
          }
          ohlcacc.reset
        }

        //--------------------------------------------------
        // reset internal data struct
        //--------------------------------------------------
        prev_bar_dt = startTime
        prev_bar_sym = tradetick.symbol
        ohlcacc = new OHLCAcc()

      }

      //--------------------------------------------------
      // ok, deal with the current trade tick for current symbol
      //--------------------------------------------------
      var timeDiffInSec = tradetick.dt.getSecondOfDay - prev_bar_dt.getSecondOfDay

      //--------------------------------------------------
      // need to seal previous bars
      //--------------------------------------------------
      while (timeDiffInSec > barIntervalInSec
        &&
        prev_bar_dt.getSecondOfDay < endTime.getSecondOfDay) {
        prev_bar_dt = prev_bar_dt.plusSeconds(barIntervalInSec)
        val curPriceBar = ohlcacc.getOHLCPriceBar
        val curBar = new OHLCBar(prev_bar_dt, prev_bar_sym, curPriceBar)
        if (ohlcacc.ready) {
          outList += curBar.toString
        }
        ohlcacc.reset
        timeDiffInSec = tradetick.dt.getSecondOfDay - prev_bar_dt.getSecondOfDay

      }
      ohlcacc.updateOHLC(tradetick.price, tradetick.volume)
      prev_bar_sym = tradetick.symbol
      outList.toList

    }

  }

  //--------------------------------------------------
  def printUsage {
    println("NAME    Data Conversion Tools")
    println("USAGE   [format] [input file] [output file] [extra param]")
    println("Supported formats:")
    println("hkex1")
    println("    from")
    println("         08002  8003630   5.290      11000U09200020140102 0")
    println("         08002  8003630   5.290       5000U09200020140102 0")
    println("         08002  8003630   5.290       4000U09200020140102 0")
    println("    to")
    println("         08002,2014-01-02,09:20:00,5.29,5.29,5.29,5.29,123000")
    println("         08002,2014-01-02,09:25:00,5.29,5.29,5.29,5.29,0")
    println("         08002,2014-01-02,09:30:00,5.29,5.3,5.2,5.25,44000")
    println("         08002,2014-01-02,09:35:00,5.25,5.45,5.25,5.44,951000")
    println("         08002,2014-01-02,09:40:00,5.44,5.46,5.33,5.33,872000")
  }

  //--------------------------------------------------
  def main(args: Array[String]) = {
    println("DataConvert starts")

    println("Input arguments are: ")
    args.foreach { s => println("  " + s) }

    val format = args(0)
    val inputFile = args(1)
    val outputFile = args(2)
    val barIntervalInSec = args(3).toInt
    val tradeDate = args(4).toInt
    val startHHMMSS = args(5).toInt
    val endHHMMSS = args(6).toInt

    val startTime = new DateTime(tradeDate / 10000, (tradeDate / 100) % 100, tradeDate % 100, startHHMMSS / 10000, (startHHMMSS / 100) % 100, startHHMMSS % 100)
    val endTime = new DateTime(tradeDate / 10000, (tradeDate / 100) % 100, tradeDate % 100, endHHMMSS / 10000, (endHHMMSS / 100) % 100, endHHMMSS % 100)
    val pw = new java.io.PrintWriter(new File(outputFile))

    //--------------------------------------------------
    // only 1 format for now
    //--------------------------------------------------
    val outputAdaptor = new OHLCOutputAdaptor(startTime, endTime, barIntervalInSec)

    for (line <- scala.io.Source.fromFile(inputFile).getLines()) {

      //--------------------------------------------------
      // only 1 format for now
      //--------------------------------------------------
      outputAdaptor.convertToOHLCOutput(parseHKExFmt1(line)).foreach { s => { pw.write(s); pw.write("\n"); pw.flush } }
    }

    //--------------------------------------------------
    // deliberately stuff a fake record at the end to flush out the last piece of data in memory
    //--------------------------------------------------
    outputAdaptor.convertToOHLCOutput(new TradeTick(startTime, "DUMMY", 0, 0)).foreach { s => { pw.write(s); pw.write("\n"); pw.flush } }

    pw.close()

    println("DataConvert ended")
  }
}
