package com.leone.bigdata.spark.scala.sql.spark2

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2019-01-07
  **/
object SparkSqlReadJdbc {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSqlReadJdbc").master("local[*]").getOrCreate()

    /*val dbTable: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://ip:3306/db01?useSSL=false",
        "driver" -> "com.mysql.jdbc.Driver",
        "user" -> "root",
        "dbtable" -> "t_student",
        "password" -> "cloud")
    ).load()*/

    val jdbcUrl = "jdbc:mysql://ip:3306/db01?useSSL=false"
    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "xxxx")

    val dbTable: DataFrame = spark.read.jdbc(jdbcUrl, "t_student", props)

    // 打印表的约束信息
    dbTable.printSchema()

    // 显示数据信息
    dbTable.show()

    // 使用函数式编程过滤
    //    val filter: Dataset[Row] = dbTable.filter(e => {
    //      e.getAs[Int](2) <= 20
    //    })
    //    filter.show()

    import spark.implicits._
    // 使用lambda的方式
    //    val r = dbTable.filter($"age" <= 19)
    //    r.show()

    val result: DataFrame = dbTable.select($"s_id" as "id", $"s_name" as "name", $"s_age" * 10 as "age")
    result.show()

    // 写回数据库
    // result.write.mode("ignore").jdbc(jdbcUrl,"t_student_bak",props)
    result.write.jdbc(jdbcUrl, "t_student_bak", props)

    // 写回到text文件
    // result.write.text("file:///root/output/spark/text")

    // 写回到json文件
    // result.write.json("file:///root/output/spark/json")

    // 写回到CSV文件
    // result.write.csv("file:///root/output/spark/csv")

    // 写回到parquet文件
    // result.write.parquet("file:///root/output/spark/parquet")

    spark.close()

  }

}
