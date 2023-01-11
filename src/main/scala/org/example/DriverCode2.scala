package org.example
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.functions.{col, desc, to_timestamp}
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
    .load("C:/Users/AparnaMenonS/IdeaProjects/SparkAssignment/src/main/resources/ghtorrent-logs.txt")
  val df2 = df.withColumn("debug_level",split(col("_c0"),",").getItem(0))
    .withColumn("timestamp",split(col("_c0"),",").getItem(1))
    .withColumn("down_id",split(col("_c0"),",").getItem(2))

//  df.printSchema()
//  println("DF2")
//  df2.printSchema()
  val cols =Seq("_c0")
//  df2.show(1,false)
  val df3= df2.withColumn("ret_stage",split(col("down_id"),"\\-\\-").getItem(1))
    .withColumn("down_id",split(col("down_id"),"\\-\\-").getItem(0))
    .drop(cols: _*) //.printSchema()
  // get item 1 to get the ret stage

//  df3.printSchema()
//    df3.show(false)

  //df4
  val df4 = df3.withColumn("rest",split(col("ret_stage"),"\\.rb\\:").getItem(1))
    .withColumn("ret_stage",split(col("ret_stage"),"\\.rb\\:").getItem(0))
    //.withColumn("rest",split(col("ret_stage"),"\\:").getItem(1))
  df4.printSchema()
  df4.show(2,false)

  Functions_2.function1(df4)
  Functions_2.functions_2(df4)
  Functions_2.functions_3(df4)
  println("Most HTTPS Requests :")
  Functions_2.function_4(df4)
  Functions_2.function_5(df4)
  //df4.show(5)
  val df5 =df4.withColumn("timestamp",split(col("timestamp"),"\\+00:00").getItem(0))
// dfDate.printSchema()
}