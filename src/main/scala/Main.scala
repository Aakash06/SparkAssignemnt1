import org.apache.log4j.Logger

object Main extends App {

  val sparkAssignment = new SparkAssignment

  val log = Logger.getLogger("Spark-Assignment1")

 //--- Finding Total Number of Records ---
  log.info("Total Number of Records : " + sparkAssignment.getTotalRecords) //7598006

  //--- Finding English Records ---
  log.info("Total Number of English Records : " + sparkAssignment.getTotalEnglishPages) //2278417


  //----- Call 10 Records -----
  val data = sparkAssignment.topRecords

  sparkAssignment.writeInFile("src/main/resources",data,"Records_10")

  //----- Call Highly Requested Data -----

  val requestedData = sparkAssignment.highRequestedData.map(data1 => data1.productIterator.mkString("\t"))

  sparkAssignment.writeInFile("src/main/resources",requestedData,"High_Requested_Records")//11 Records

}
