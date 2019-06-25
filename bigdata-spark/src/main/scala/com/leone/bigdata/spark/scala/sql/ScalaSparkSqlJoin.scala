package com.leone.bigdata.spark.scala.sql

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-21
  **/
object ScalaSparkSqlJoin {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("join").master("local[*]").getOrCreate()
    // 导入隐式转换
    import spark.implicits._

    // 表一
    val person: Dataset[String] = spark.createDataset(List("1,jack,China", "2,andy,America", "3,james,Japan"))
    val personDataset: Dataset[(Long, String, String)] = person.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val country = fields(2)
      (id, name, country)
    })
    val personDataFrame = personDataset.toDF("user_id", "username", "country")


    // 表二
    val country: Dataset[String] = spark.createDataset(List("China,中国", "America,美国", "Japan,日本"))
    val countryDataFrame = country.map(line => {
      val fields = line.split(",")
      val code = fields(0)
      val name = fields(1)
      (code, name)
    }).toDF("name", "zh_name")

    // 表一表二关联
    personDataFrame.createTempView("tv_user")
    countryDataFrame.createTempView("tv_order")

    spark.sql("select user_id, username, country, zh_name from tv_user join tv_country on tv_user.country = tv_country.name").show()

//    val result = personDataFrame.join(countryDataFrame, $"country" === $"name")
//    result.select("user_id", "username", "country", "zh_name").show()

    spark.stop()
  }

}
