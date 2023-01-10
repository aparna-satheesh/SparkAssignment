package org.example
import org.apache.spark.sql.SparkSession
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.SQLContext
//import sparkObject.spark.implicits._
import org.apache.spark.sql.functions.split

object DriverCode2 extends App {
  val spark1 = SparkSession.builder().master("local[1]").appName("spark session example").getOrCreate()
  spark1.sparkContext.setLogLevel("FATAL")

  import spark1.implicits._

  //  case class LogLine(debug_level: String, timestamp: Date, download_id: Integer,retrieval_stage: String, rest: String);
  //  val dateFormat = "yyyy-MM-dd:HH:mm:ss"
  //  val regex = """([^\s]+), ([^\s]+)\+00:00, ghtorrent-([^\s]+) -- ([^\s]+).rb: (.*$)""".r

  val rdd = spark1.read.
    text("C:/Users/AparnaMenonS/IdeaProjects/untitled5/src/main/resources/log1.txt")
  rdd.show()
  val df = rdd.toDF()
//  df.show()
//  df['debug_level], df['timestamp'], df['download_id''],df['retrieval_stage''],df['rest']= df['value'].split('\t')

}