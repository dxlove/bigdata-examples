package com.andy.spark.sql.spark2

import org.apache.spark.sql.SparkSession

/**
  * <p>untyped (弱类型) 操作
  *
  * @author leone
  * @since 2019-03-31
  **/
object SparkSql2xUntyped {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSql2xOnHive").master("local[*]").getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    // 加载两份数据型成两个dataframe

    val department = spark.read.json("file:///D:\\root\\logs\\join\\department.json")
    val employee = spark.read.json("file:///D:\\root\\logs\\join\\employee.json")

    // 进行计算操作

    // 对employee进行过滤 然后join ()
    employee.filter("age > 20")
      // untyped join两个表的字段连接条件需要使用三个等号
      .join(department, $"depId" === $"id")
      // 根据部门名称和员工姓名进行分组
      .groupBy(department("name"), employee("gender"))
      // 使用聚合函数
      .agg(avg(employee("salary")), avg(employee("age")))
      // 打印结果
      .show()

    spark.stop()
  }

}
