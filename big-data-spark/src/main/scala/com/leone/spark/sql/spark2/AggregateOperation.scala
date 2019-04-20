package com.leone.spark.sql.spark2

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-04-01
  **/
object AggregateOperation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("AggregateOperation").master("local[*]").getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 加载两份数据型成两个dataframe
    val department = spark.read.json("file:///root/logs/join/department.json")
    val employee = spark.read.json("file:///root/logs/join/employee.json")

    employee.join(department, $"depId" === $"id")
      .groupBy(department("name"))
      .agg(avg(employee("salary")), sum(employee("salary")), max(employee("salary")), min(employee("salary")), count(employee("name")), countDistinct(employee("name")))
      .show()

    employee.groupBy(employee("depId"))
        .agg(collect_set(employee("name")),collect_list(employee("name")))
        .collect()
        .foreach(println)

    spark.stop()

  }

}
