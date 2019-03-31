package com.andy.spark.sql.spark2

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-20
  **/
object SparkSqlReadJson {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSqlReadJson")
      .master("local[*]")
      // spark sql 2.x 需要手动设置 spark sql 原数据的存储目录
      // .config("spark.sql.warehouse.dir", "file:///d://tmp/spark/warehouse")
      .getOrCreate()

    // {"user_id":4,"username":"郑揩公","sex":"女","age":48,"deleted":false,"tel":"13912350867","create_time":"2010-08-31 09:49:05","integral":9282}
    val dataFrame = spark.read.json("file:///d:/root/logs/json/")

    dataFrame.show(10)

    dataFrame.createTempView("t_user")

    spark.sql("select user_id,username,age,sex,tel,create_time,integral from t_user where age < 30 order by user_id asc limit 50").show()

    dataFrame.show(10)

    dataFrame.printSchema()

    dataFrame.select("user_id", "age").show(10)

    import spark.implicits._

    // 查询某些字段
    dataFrame.select($"user_id", $"username", $"age", $"age" + 1).show()

    // filter 过滤操作
    dataFrame.filter($"age" > 50).show()

    // group 分组然后聚合
    dataFrame.groupBy("age").count().orderBy("age").where("age < 18").show()



    spark.stop()
  }

}
