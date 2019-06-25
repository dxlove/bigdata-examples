package com.leone.bigdata.spark.scala.sql

import breeze.util.Encoder
import com.leone.bigdata.spark.scala.caseclass.User
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2019-06-25
  **/
object ScalaDatasetCreate {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("dataset").master("local[*]").getOrCreate()

    import spark.implicits._

    // 第一种方式创建
    //val dataset = spark.read.json(args(0)).as[User]

    val stringRDD = spark.sparkContext.textFile(args(0))

    val userRDD = stringRDD.map(_.split(",")).map(s => {
      User(s(0).toLong, s(1), s(2).toInt, s(3).toInt, s(4).toDouble, s(5), s(6).toBoolean)
    })

    // 第二种方式创建
    val dataset = spark.createDataset(userRDD)

    dataset.select("userId", "username", "sex").where("sex = 1").orderBy("credit").show()

    dataset.show()

    dataset.printSchema()

    dataset.createOrReplaceTempView("t_user")

    // 获取 spark sql 的执行计划
    spark.sql("select * from t_user where age < 18").explain()

    // dataSet 转换为 dataFrame
    val dataFrame = dataset.toDF

    // dataset 转换为 rdd
    val rdd = dataset.rdd

    spark.stop()
  }

}
