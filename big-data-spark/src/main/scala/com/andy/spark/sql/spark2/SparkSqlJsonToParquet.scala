package com.andy.spark.sql.spark2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-20
  **/
object SparkSqlJsonToParquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("jsonToParquet").master("local[*]").getOrCreate()

    val dataFrame = spark.read.format("parquet").load("file:///d:/root/logs/parquet/")

    dataFrame.show(10)

    dataFrame.printSchema()

    dataFrame.write.format("json").save("file:///d:/tmp/spark/output1")

    spark.stop()
  }

}
