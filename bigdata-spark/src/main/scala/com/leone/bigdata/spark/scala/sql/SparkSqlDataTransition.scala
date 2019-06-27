package com.leone.bigdata.spark.scala.sql

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-20
  **/
object SparkSqlDataTransition {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSqlDataTransition").master("local[*]").getOrCreate()

    // 从不同类型文件中创建 dataFrame
    val dataFrame = spark.read.json(args(0))
    // val dataFrame = spark.read.parquet(args(0))
    // val dataFrame = spark.read.csv(args(0))
    // val dataFrame = spark.read.text(args(0))
    // val dataFrame = spark.read.orc(args(0))

    dataFrame.show(10)
    dataFrame.printSchema()

    // 将结果保存为不同类型的结果文件
    dataFrame.write.parquet(args(1))
    dataFrame.write.json(args(1))
    dataFrame.write.csv(args(1))
    dataFrame.write.orc(args(1))

    spark.stop()
  }

}
