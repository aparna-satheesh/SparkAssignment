package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructType,StringType}

object DriverCode1 extends App {

  val spark = SparkSession.builder().master("local[1]").appName("spark session example").getOrCreate()
  spark.sparkContext.setLogLevel("FATAL")
//  println("App Name " + spark.sparkContext.appName)
//  println("App Name " + spark.sparkContext.master)

//  var z = spark.sparkContext.parallelize(List(1, 2, 3, 4, 5))
//  z.foreach(println)
//  val df = spark.read.csv("C:\\Users\\AparnaMenonS\\IdeaProjects\\untitled5\\src\\main\\resources\\user.csv")
//  df.show()
  val schema_user = new StructType()
    .add("userid",IntegerType,true)
    .add("emailid",StringType,true)
    .add("nativelanguage",StringType,true)
    .add("location",StringType,true)
  val df_user = spark.read.format("csv")
    .option("header", "true")
    .schema(schema_user)
    .load("C:/Users/AparnaMenonS/IdeaProjects/untitled5/src/main/resources/user.csv")
  df_user.printSchema()
  df_user.show()

//transaction_id,product_id,userid,price,product_description
  val schema_trans = new StructType()
    .add("transaction_id", IntegerType, true)
    .add("product_id", IntegerType, true)
    .add("userid", IntegerType, true)
    .add("price", IntegerType, true)
    .add("product_description", StringType,true)
  val df_trans = spark.read.format("csv")
    .option("header", "true")
    .schema(schema_trans)
    .load("C:/Users/AparnaMenonS/IdeaProjects/untitled5/src/main/resources/transaction.csv")
  df_trans.printSchema()
  df_trans.show()
  //1
  Functions_1.function1(df_user)
//  println("Distinct Location Count: " + df_user
//    .select("location")
//    .distinct
//    .count())
    //2
//  //Joining 2 dfs using inner
  val joinCondition = df_user.col("userid")===df_trans.col("userid")
  val df_userTrans = df_user.join(df_trans,joinCondition,"inner").drop(df_trans("userid"))
  df_userTrans.show()
//  df_userTrans.groupBy("userid","product_description").count().show()
  Functions_1.function2(df_userTrans)
  //3
//  df_userTrans.groupBy("userid").sum("price").as("Total Spending").orderBy("userid").show()
  Functions_1.function3(df_userTrans)

}
