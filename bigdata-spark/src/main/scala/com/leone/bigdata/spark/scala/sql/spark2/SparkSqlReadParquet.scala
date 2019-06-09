package com.leone.bigdata.spark.scala.sql.spark2

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-20
  **/
object SparkSqlReadParquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSqlReadJson")
      .master("local[*]")
      .getOrCreate()

    val dataFrame = spark.read.parquet("file:///root/logs/parquet/")

    dataFrame.show(10)

    dataFrame.printSchema()

    spark.stop()
  }


}
