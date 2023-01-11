package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, desc, split}

object Functions_2 extends App{

  def function1(df: DataFrame): Unit = {
    print("No of entries in rdd:"+df.count())
  }
  def functions_2(df:DataFrame):Unit={
    println("\nNo of Warning messages: "+df.filter(df("debug_level")==="WARN").count)
  }

  def functions_3(df4:DataFrame):Unit={
    println("No of API client logs : "+ df4.filter(df4("ret_stage")=== " api_client").count)
    val df5 = df4.filter(df4("ret_stage") === " api_client")
//    df5.select("rest").show(5, false)
    val df_temp = df5.withColumn("repo", split(col("rest"), "repos\\/").getItem(1))
      .withColumn("repo", split(col("repo"), "/").getItem(0))
      .drop("debug_level", "timestamp", "down_id","ret_stage","rest")
//    df_temp.show(10, false)
    println("No of unique Repos :"+ df_temp.distinct().count())
  }
  def function_4(df4:DataFrame): Unit = {
    val df_temp = df4.filter(col("rest").contains("http"))
      .groupBy("ret_stage").count().orderBy(desc("count"))
      .drop("debug_level")
    print(df_temp.select(df_temp.columns.slice(0,1).map(col(_)):_*))
//    df.select(df.columns.slice(0, 1).map(col(_)): _*)
//    return x
  }
}
