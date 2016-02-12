import org.nirvana._
import java.io._
import org.joda.time.{Period, DateTime, Duration}


object DataConvert {

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
    val outputAdaptor = new DataFmtAdaptors.OHLCOutputAdaptor(startTime, endTime, barIntervalInSec)

    for (line <- scala.io.Source.fromFile(inputFile).getLines()) {

      //--------------------------------------------------
      // only 1 format for now
      //--------------------------------------------------
      outputAdaptor.convertToOHLCOutput(DataFmtAdaptors.parseHKExFmt1(line)).foreach { s => { pw.write(s); pw.write("\n"); pw.flush } }
    }

    //--------------------------------------------------
    // deliberately stuff a fake record at the end to flush out the last piece of data in memory
    //--------------------------------------------------
    outputAdaptor.convertToOHLCOutput(new org.nirvana.TradeTick(startTime, "DUMMY", 0, 0)).foreach { s => { pw.write(s); pw.write("\n"); pw.flush } }

    pw.close()

    println("DataConvert ended")
  }
}
