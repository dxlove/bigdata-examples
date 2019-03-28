package com.andy.spark.structured

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-03-28
  **/
object StructuredWordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("structured").master("local[*]").getOrCreate()

    import spark.implicits._

    val line = spark.readStream
      .format("socket")
      .option("host", "node-7")
      .option("port", "9999")
      .load()

    val words = line.as[String].flatMap(_.split(" "))

    val wordsCount = words.groupBy("value").count()

    val query = wordsCount.writeStream.outputMode("complete").format("console").start()

    query.awaitTermination()

  }

}
