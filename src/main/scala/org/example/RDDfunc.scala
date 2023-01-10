package org.example

import org.apache.spark.sql.functions.col
//import org.apache.spark.sql
import java.text.SimpleDateFormat
import java.util.Date
import org.example.DriverCode2.spark1

object RDDfunc extends App {

//  case class LogLine(debug_level: String, timestamp: Date, download_id: Integer,
//                     retrieval_stage: String, rest: String);

  val df = spark1.read.format("csv") // Use "csv" for both TSV and CSV
    .option("header", "true")
    .option("delimiter", "\t") // Set delimiter to tab .
    .load("C:/Users/AparnaMenonS/IdeaProjects/SparkAssignment/src/main/resources/log1.txt")
    df.printSchema()
//    .withColumn(col("_tmp").split(col("value"), "\\,"))
//    .withColumn(col("_tmp").getItem(0).as("City"))
//    .withColumn(col("_tmp").getItem(1).as("State"))
//    .drop("_tmp")
//    .drop("Address")
  import spark1.sqlContext.implicits._
}
