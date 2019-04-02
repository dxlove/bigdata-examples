package com.andy.spark.structured

import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * <p>structured 整合kafka
  *
  * @author leone
  * @since 2019-03-29
  **/
object StructuredWithKafka {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("structured").master("local[*]").getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "node-2:9092,node-3:9092,node-4:9092")
      .option("subscribe", "structured-topic")
      //.option("startingOffsets", "earliest")
      .load()

    val kafkaDS: Dataset[(String, String)] = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    println(kafkaDS)

    kafkaDS.printSchema()

    val words = kafkaDS.flatMap(_._2.split(" "))
    val wordCounts = words.groupBy("value").count()
    val query = wordCounts
      .writeStream
      .outputMode(OutputMode.Complete())
      .format("console")
      .start()
    query.awaitTermination()

  }

}
