
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkConfig {

  Logger.getLogger("org").setLevel(Level.OFF)

  val customSchema = StructType(Array(
    StructField("Id", IntegerType, nullable = true),
    StructField("Name", StringType, nullable = true)

  ))

  val sparkConf: SparkConf = new SparkConf().setMaster("local[*]")
    .setAppName("SparkAssignment")

  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate

  val sc: SparkContext = sparkSession.sparkContext

  val pathFile = "src/main/resources/pagecounts-20151201-220000"

}
