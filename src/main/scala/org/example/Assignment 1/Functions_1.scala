package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions.sum
//import org.example.Demo123.{df_trans, df_user}


object Functions_1 extends App {

    def function1(df_user: DataFrame): Unit = {
        println("Distinct Location Count: " + df_user
          .select("location")
          .distinct
          .count())
    }

    def function2(df_userTrans:DataFrame): = {
        df_userTrans.groupBy("userid", "product_description").count().orderBy("userid").show()
    }

    def function3(df_userTrans:DataFrame): Unit = {
        df_userTrans.groupBy("userid")
        .agg(
            sum("price").as("Total Spending")).orderBy("userid")
          .show()
    }

}
