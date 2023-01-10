package org.example
import org.apache.spark.sql.SparkSession

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col
//import sparkObject.spark.implicits._
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.trim

object DriverCode2 extends App {
  val spark1 = SparkSession.builder().master("local[1]").appName("spark session example").getOrCreate()
  spark1.sparkContext.setLogLevel("FATAL")

  import spark1.implicits._

  val df = spark1.read.format("csv") // Use "csv" for both TSV and CSV
    .option("header", "true")
    .option("delimiter", "\t") // Set delimiter to tab
    .option("header",false)
    .load("C:/Users/AparnaMenonS/IdeaProjects/SparkAssignment/src/main/resources/log1.txt")
  val df2 = df.withColumn("debug_level",split(col("_c0"),",").getItem(0))
    .withColumn("timestamp",split(col("_c0"),",").getItem(1))
    .withColumn("down_id",split(col("_c0"),",").getItem(2))
//    .withColumn("rest",split(col("_c0"),"").getItem(3))
val df3 = df2.withColumn("ret_stage",split(col("down_id"),":").getItem(0))
//  .split(col("_c0"),"\\,").getItem(1).as("timestamp")
//  .split(col("_c0"),"\\--").getItem(2).as("down_id")
//  .split(col("_c0"),"\\.").getItem(3).as("down_id"))

  df.printSchema()
  df2.printSchema()
  df3.printSchema()
//  df2.show(1,false)
//  df.show(1)
  df3.show(1,false)
  val cols =Seq("_c0")
  val df4= df3.withColumn("ret_stage",split(col("down_id"),"\\-\\-").getItem(0))
    .drop(cols: _*) //.printSchema()
  // get item 1 to get the ret stage
//  df3.drop(col("down_id")).printSchema()
  df4.printSchema()
    df4.show(false)
}