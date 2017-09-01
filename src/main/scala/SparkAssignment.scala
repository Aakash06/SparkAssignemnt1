import java.io.{File, PrintWriter}

import SparkConfig._
import org.apache.spark.rdd.RDD

class SparkAssignment {

  def writeInFile(directoryPath : String,content : List[String],fileName: String): Unit ={

    val writer = new PrintWriter(new File(directoryPath + File.separator + fileName + ".txt"))
    writer.write(content.mkString("\n"))
    writer.close()
  }

  val pageCounts: RDD[String] = sc.textFile(pathFile)

  def getTotalEnglishPages: Int = {
    getEnglishPages.length
  }

  def getEnglishPages: Array[String] = {
    pageCounts.filter(_.split(" ")(0).contentEquals("en")).collect()
  }

  def topRecords: List[String] = {
    pageCounts.zipWithIndex().filter(_._2 < 10).keys.collect().toList
  }

  def getTotalRecords: Long = {
    pageCounts.count()
  }

  def highRequestedData: List[(String, Long)] = {
    pageCounts.map(data=> (data.split(" ")(1),data.split(" ")(2).toLong))
      .reduceByKey(_ + _).filter(request => request._2 > 200000).collect().toList
  }

}
