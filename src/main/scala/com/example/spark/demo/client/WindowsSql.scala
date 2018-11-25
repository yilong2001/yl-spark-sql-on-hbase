package com.example.spark.demo.client

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
  * Created by yilong on 2018/9/30.
  */
object WindowsSql {
  def main(args: Array[String]) : Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[3]")
    conf.setAppName("SqlExample")

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config(conf)
      .getOrCreate()

    val sQLContext = spark.sqlContext

    import sQLContext.implicits._

    val userWindow = Window.partitionBy("user_name").orderBy("login_date")

    val userSessionWindow = Window.partitionBy("user_name", "session")

    val newSession =  (coalesce(datediff($"login_date", lag($"login_date", 1).over(userWindow)), lit(0)) > 5).cast("bigint")

    val  df = Seq(
      ("SirChillingtonIV", "2012-01-04"), ("Booooooo99900098", "2012-01-04"),
      ("Booooooo99900098", "2012-01-06"), ("OprahWinfreyJr", "2012-01-10"),
      ("SirChillingtonIV", "2012-01-11"), ("SirChillingtonIV", "2012-01-14"),
      ("SirChillingtonIV", "2012-08-11")
    ).toDF("user_name", "login_date")

    val sessionized = df.withColumn("session", sum(newSession).over(userWindow))

    val result = sessionized
      .withColumn("became_active", min($"login_date").over(userSessionWindow))
      //.drop("session")

    println("=================================================")
    println(result.queryExecution.logical)
    println(result.queryExecution.analyzed)
    //subdf.queryExecution.analyzed
    println(result.queryExecution.optimizedPlan)
    println(result.queryExecution.executedPlan)
    println(result.queryExecution.sparkPlan)

    val a = sQLContext.range(10).select(col("id"), lit(0).as("count"))
    val b = sQLContext.range(10).select((col("id") % 3).as("id")).groupBy("id").count()
    val c = a.join(b, a("id") === b("id"), "left_outer").filter(b("count").isNull)

    println("=================================================")
    println(c.queryExecution.logical)
    println(c.queryExecution.analyzed)
    //subdf.queryExecution.analyzed
    println(c.queryExecution.optimizedPlan)
    println(c.queryExecution.executedPlan)
    println(c.queryExecution.sparkPlan)

    a.show()
    b.show()
    c.show()
    //result.show()
  }
}
