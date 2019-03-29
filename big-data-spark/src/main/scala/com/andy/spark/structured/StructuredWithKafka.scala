package com.andy.spark.structured

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-03-29
  **/
object StructuredWithKafka {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("structured").master("local[*]").getOrCreate()

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node-2:9092,node-3:9092,node-4:9092")
      .option("subscribe", "structured-topic")
      .load()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

  }

}
