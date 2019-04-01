package com.andy.spark.sql.spark2

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-20
  **/
object SparkSqlReadCsv {

  case class User(user_id: Long, username: String, age: Long, sex: String, tel: String, create_time: String, integral: Long) {}

  case class Person(person_id: Long, age: Int, sex: Int) {}

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSqlReadJson")
      .master("local[*]")
      .getOrCreate()

    // 基于已有的结构化数据构造 dataFrame
    val dataFrame = spark.read.csv("file:///d:/root/logs/csv/")

     dataFrame.show(10)

    dataFrame.printSchema()

    // 创建零时视图
    dataFrame.createTempView("t_user")

    spark.stop()
  }



}
