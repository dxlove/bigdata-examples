package com.leone.bigdata.spark.scala.sql

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-20
  **/
object SparkSqlReadCsv {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSqlReadJson")
      .master("local[*]")
      .getOrCreate()

    // 基于已有的结构化数据构造 dataFrame
    val dataFrame = spark.read.csv("file:///root/logs/csv/")

    dataFrame.show(10)

    dataFrame.printSchema()

    import spark.implicits._

    dataFrame.select($"_c0" as "id", $"_c1" as "name").show(6)

    // 创建零时视图
    dataFrame.createTempView("t_user")

    spark.sql("select * from t_user limit 8").show()

    spark.stop()
  }


}
