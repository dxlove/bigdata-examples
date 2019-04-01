package com.andy.spark.sql.spark2

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-04-01
  **/
object BasicOperation {

  case class User(user_id: Long, username: String, age: Long, sex: String, tel: String, create_time: String, integral: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("BasicOperation").master("local[*]").getOrCreate()

    val dataFrame = spark.read.format("json").load("file:///root/logs/json")


    println(dataFrame.count())
    dataFrame.cache()

    println(dataFrame.count())

    // 创建零时视图
    dataFrame.createOrReplaceTempView("t_user")
    spark.sql("select * from t_user where age < 19").show()

    // 获取 spark sql 的执行计划
    spark.sql("select * from t_user where age < 18").explain()

    import spark.implicits._
    val userDS = dataFrame.as[User]

    userDS.printSchema()
    
    spark.stop()

  }

}
