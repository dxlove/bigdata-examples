package com.andy.spark.sql.spark2

import org.apache.spark.sql.{Row, SparkSession}

import scala.math.BigDecimal.RoundingMode

/**
  * UDAF：User Defined Aggregate Function。用户自定义聚合函数。是Spark 1.5.x引入的最新特性。
  * UDF，其实更多的是针对单行输入，返回一个输出
  * 这里的UDAF，则可以针对多行输入，进行聚合计算，返回一个输出，功能更加强大
  *
  * @author leone
  * @since 2018-12-23
  */
object SparkSqlUDF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()

    // 写一个Double数据格式化的自定义函数(给定保留多少位小数部分)
    spark.sqlContext.udf.register(
      "doubleValueFormat", // 自定义函数名称
      (value: Double, scale: Int) => {
        // 自定义函数处理的代码块
        BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_DOWN).doubleValue()
      })

    // 自定义UDAF
    spark.udf.register("selfAvg", AvgUDAF)

    spark.sql(
      """
        |SELECT
        |  deptno,
        |  doubleValueFormat(AVG(sal), 2) AS avg_sal,
        |  doubleValueFormat(selfAvg(sal), 2) AS self_avg_sal
        |FROM hadoop09.emp
        |GROUP BY deptno
      """.stripMargin).show()

    spark.stop()
  }

}
