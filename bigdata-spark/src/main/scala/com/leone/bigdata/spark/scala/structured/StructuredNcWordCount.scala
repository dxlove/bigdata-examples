package com.leone.bigdata.spark.scala.structured

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode

/**
  * <p>
  *
  * @author leone
  * @since 2019-03-28
  **/
object StructuredNcWordCount {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("structured").master("local[*]").getOrCreate()

    import spark.implicits._

    val line = spark.readStream
      .format("socket")
      .option("host", "39.108.125.41")
      .option("port", "8000")
      .load()

    // 由于控制台日志打印太多方便调试
    spark.sparkContext.setLogLevel("WARN")

    val words = line.as[String].flatMap(_.split(" "))

    val wordsCount = words.groupBy("value").count()

    // 注意：Append模式不支持基于数据流上的聚合操作（Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets）
    // 三种模式1：complete 所有内容都输出  2：append 新增的行才输出  3：update 更新的行才输出
    val query = wordsCount.writeStream.outputMode(OutputMode.Complete()).format("console").start()

    query.awaitTermination()

  }

}
