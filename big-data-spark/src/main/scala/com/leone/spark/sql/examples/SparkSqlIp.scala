package com.leone.spark.sql.examples

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-21
  **/
object SparkSqlIp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSqlIp").master("local[*]").getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("hdfs://node-1:9000/spark/input2")

    import spark.implicits._

    // 把数据和约束信息绑定到一起
    val df = lines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3)
      val province = fields(6)
      (startNum, endNum, province)
    }).toDF("snum", "enum", "province")

    df.show()

    spark.stop()
  }

}
