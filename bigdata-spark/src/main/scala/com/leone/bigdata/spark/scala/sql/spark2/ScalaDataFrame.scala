package com.leone.bigdata.spark.scala.sql.spark2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * <p>
  *
  * @author leone
  * @since 2019-01-10
  **/
object ScalaDataFrame {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("dataFrame").master("local[*]").getOrCreate()

    val array: Array[String] = Array("1,james,12", "2,jack,23", "3,andy,34")

    val rdd: RDD[String] = spark.sparkContext.makeRDD(array)

    val mapedRDD: RDD[User] = rdd.map(e => {
      val arr = e.split(",")
      User(arr(0).toInt, arr(1), arr(2).toInt)
    })

    val dataFrame: DataFrame = spark.createDataFrame(mapedRDD)

    dataFrame.printSchema()
    dataFrame.show()
    dataFrame.createTempView("t_user")

    spark.sql("select * from t_user where age > 20 order by age desc limit 10").show()

    dataFrame.selectExpr("id", "name").show()
    dataFrame.where("age > 20").show()
    dataFrame.agg(sum("age"), sum("id")).show()

  }
  case class User(id: Int, name: String, age: Int) {}
}

