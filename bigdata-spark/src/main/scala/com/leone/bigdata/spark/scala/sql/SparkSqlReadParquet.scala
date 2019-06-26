package com.leone.bigdata.spark.scala.sql

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-20
  **/
object SparkSqlReadParquet {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("readJson").master("local[*]").getOrCreate()

    val dataFrame = spark.read.parquet(args(0))

    dataFrame.select(dataFrame.col("userId"), dataFrame.col("age"), dataFrame.col("sex")).show(20)

    dataFrame.filter(dataFrame.col("age") > 24).show()

    dataFrame.groupBy("age").count().show()

    dataFrame.printSchema()

    spark.stop()
  }


}
