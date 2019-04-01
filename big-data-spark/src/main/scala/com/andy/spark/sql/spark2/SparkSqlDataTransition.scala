package com.andy.spark.sql.spark2

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-20
  **/
object SparkSqlDataTransition {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSqlDataTransition")
      .master("local[*]")
      .getOrCreate()

    val dataFrame = spark.read.json("file:///d:/root/logs/json/")
    // val dataFrame = spark.read.parquet("file:///d:/root/logs/parquet/")
    // val dataFrame = spark.read.csv("file:///d:/root/logs/csv/")
    // val dataFrame = spark.read.text("file:///d:/root/logs/text/")
    // val dataFrame = spark.read.orc("file:///d:/root/logs/orc/")

    dataFrame.show(10)

    dataFrame.printSchema()

    dataFrame.write.parquet("file:///d:/root/output/parquet/")
    dataFrame.write.json("file:///d:/root/output/json/")
    dataFrame.write.csv("file:///d:/root/output/csv/")
    dataFrame.write.orc("file:///d:/root/output/orc/")

    spark.stop()
  }

}
