package org.example

import org.example.DriverCode2.spark1

object Functions_2 extends App{

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
