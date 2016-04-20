import org.nirvana._
import java.io._
import org.joda.time.{Period, DateTime, Duration}

object DataConvert {

  //--------------------------------------------------
  def printUsage {
    println("NAME    Data Conversion Tools")
    println("USAGE   [input file] [output file] [conversion fmt] [extra params ...]")
    println
    println("Supported formats:")
    println("hkex1")
    println("         08002  8003630   5.290      11000U09200020140102 0")
    println("         08002  8003630   5.290       5000U09200020140102 0")
    println("         08002  8003630   5.290       4000U09200020140102 0")
    println("cashohlcfeed")
    println("         20160217_100000_000000,ohlcfeed,HKSE,00014,29.995,30.25,29.8,29.9,187000")
    println("         20160217_100000_000000,ohlcfeed,HKSE,00016,84.1,86.0,83.07,85.8,629150")
    println("         20160217_100000_000000,ohlcfeed,HKSE,00017,6.08,6.2,6.08,6.2,1151100")
    println("         20160217_100000_000000,ohlcfeed,HKSE,00018,0.79,0.79,0.79,0.79,22000")
    println("         20160217_100000_000000,ohlcfeed,HKSE,00019,74.3,74.8,73.8,74.2,193693")
    println("         20160217_100000_000000,ohlcfeed,HKSE,00020,29.2,29.9,29.15,29.45,163000")
    println("         20160217_100000_000000,ohlcfeed,HKSE,00023,22.1,22.75,22.1,22.7,596508")
    println("         20160217_100000_000000,ohlcfeed,HKSE,00024,0.26,0.26,0.26,0.26,12000")
    println("cashmdi")
    println("         19900102_160000_000000,00941,80.1,400,B,80.1,1000,999999,0,999999,0,999999,0,999999,0,A,80.15,1000,999999,0,999999,0,999999,0,999999,0")
    println
    println("Supported conversions:")
    println("         hkex1_cashohlcfeed")
    println("         hkex1_cashmdi")
    println("         cashohlcfeed_cashmdi")
  }

  //--------------------------------------------------
  def main(args: Array[String]) = {
    println("DataConvert starts")

    println("Input arguments are: ")
    args.foreach { s => println("  " + s) }

    //--------------------------------------------------
    // read arguments
    //--------------------------------------------------
    val inputFile = args(0)
    val outputFile = args(1)
    val conversionFmt = args(2)
    //--------------------------------------------------

    val pw = new java.io.PrintWriter(new File(outputFile))

    if (conversionFmt == "hkex1_cashohlcfeed") {
      val barIntervalInSec = args(3).toInt
      val tradeDate = args(4).toInt
      val startHHMMSS = args(5).toInt
      val endHHMMSS = args(6).toInt
      val startTime = new DateTime(tradeDate / 10000, (tradeDate / 100) % 100, tradeDate % 100, startHHMMSS / 10000, (startHHMMSS / 100) % 100, startHHMMSS % 100)
      val endTime = new DateTime(tradeDate / 10000, (tradeDate / 100) % 100, tradeDate % 100, endHHMMSS / 10000, (endHHMMSS / 100) % 100, endHHMMSS % 100)

      val outputAdaptor = new DataFmtAdaptors.OHLCOutputAdaptor(startTime, endTime, barIntervalInSec)

      for (line <- scala.io.Source.fromFile(inputFile).getLines()) {
        outputAdaptor.convertToOHLCOutput(DataFmtAdaptors.parseHKExFmt1(line)).foreach {
          s =>
            {
              pw.write(s);
              pw.write("\n");
              pw.flush
            }
        }
      }

      //--------------------------------------------------
      // deliberately stuff a fake record at the end to flush out the last piece of data in memory
      //--------------------------------------------------
      outputAdaptor.convertToOHLCOutput(new org.nirvana.TradeTick(startTime, "DUMMY", 0, 0)).foreach { s => { pw.write(s); pw.write("\n"); pw.flush } }
    }
    else if (conversionFmt == "hkex1_cashmdi") {
      for (line <- scala.io.Source.fromFile(inputFile).getLines()) {
        pw.write(DataFmtAdaptors.convertToCashMDI(DataFmtAdaptors.parseHKExFmt1(line)))
        pw.write("\n")
        pw.flush

      }
    }
    else if (conversionFmt == "cashohlcfeed_cashmdi") {
      for (line <- scala.io.Source.fromFile(inputFile).getLines()) {
        pw.write(DataFmtAdaptors.convertFromOHLCBarToCashMDI(DataFmtAdaptors.parseCashOHLCFmt1(line, false).get))
        pw.write("\n")
        pw.flush

      }
    }

    pw.close()

    println("DataConvert ended")
  }
}

