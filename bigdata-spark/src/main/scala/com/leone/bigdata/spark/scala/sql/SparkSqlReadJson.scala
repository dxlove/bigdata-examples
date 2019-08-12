package com.leone.bigdata.spark.scala.sql

import com.leone.bigdata.spark.scala.caseclass.{Order, User}
import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-02-20
  **/
object SparkSqlReadJson {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSqlReadJson").master("local[*]").getOrCreate()

    // 基于已有的结构化数据构造 dataFrame
    val dataFrame = spark.read.json(args(0))

    dataFrame.printSchema()

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

    // 通过jvm的 object 来构造 dataset
    val orderDataset = Seq(Order(1L, 2L, "iphone", 2, 300.00, 150.00, "2019-01-23 12:00:00")).toDF()


    // 基于已有的结构化数据构造dataset 首先获取的是一个dataFrame然后使用as将 dataFrame 转换为dataset
    var userDataset = spark.read.json(args(2)).as[User]

    userDataset.show(5)

    spark.stop()
  }


}
