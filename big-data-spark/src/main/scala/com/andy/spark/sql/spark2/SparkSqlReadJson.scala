package com.andy.spark.sql.spark2

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-20
  **/
object SparkSqlReadJson {

  case class User(user_id: Long, username: String, age: Long, sex: String, tel: String, create_time: String, integral: Long) {}

  case class Person(person_id: Long, age: Int, sex: Int) {}

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSqlReadJson")
      .master("local[*]")
      .getOrCreate()

    // 基于已有的结构化数据构造 dataFrame
    // {"user_id":4,"username":"郑揩公","sex":"女","age":48,"deleted":false,"tel":"13912350867","create_time":"2010-08-31 09:49:05","integral":9282}
    val dataFrame = spark.read.json("file:///root/logs/json/")

    // dataFrame.show(10)

    dataFrame.printSchema()

    // dataFrame.select("user_id", "age").show(10)

    import spark.implicits._

    // 查询某些字段
    // dataFrame.select($"user_id", $"username", $"age", $"age" + 1).show()

    // filter 过滤操作
    // dataFrame.filter($"age" > 50).show()

    // group 分组然后聚合
    // dataFrame.groupBy("age").count().orderBy("age").where("age < 18").show()

    // 创建零时视图
    dataFrame.createTempView("t_user")

    // spark 的 sql 函数查询零时视图
    spark.sql("select user_id,username,age,sex,tel,create_time,integral from t_user where age < 30 order by user_id asc limit 50").show()

    // 通过jvm的 object 来构造 dataSet
    // User(101, "james", 18, 1, "15909876789", "2019-03-31 12:23:33", 19)
    val caseClassDS = Seq(Person(1, 3, 4)).toDF()

    // caseClassDS.show(10)

    // caseClassDS.map(_ + 1).show()

    // 基于已有的结构化数据构造dataset 首先获取的是一个dataframe然后使用as将dataframe转换为dataset
    var userDS = spark.read.json("file:///root/logs/json/").as[User]

    userDS.show(5)

    spark.stop()
  }



}
