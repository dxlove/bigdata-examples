package com.leone.spark.sql.spark2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-01-10
  **/
object DataFrameTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("sparkSql").master("local[*]").getOrCreate()

    val array: Array[String] = Array("1,james,12", "2,jack,23", "3,andy,34")

    val rdd: RDD[String] = spark.sparkContext.makeRDD(array)

    val rdd2 = rdd.map(e => {
      val arr = e.split(",")
      Customer(arr(0).toInt, arr(1), arr(2).toInt)
    })

    val frame = spark.createDataFrame(rdd2)
    frame.printSchema()
    frame.show()
    frame.createTempView("customer")

    val sql = spark.sql("select * from customer where age > 20 order by age desc")
    sql.show()

    //    frame.selectExpr("id", "name").show()

    //    frame.where("age > 20").show()

    //    frame.agg(sum("age"),sum("id"))

  }

  case class Customer(id: Int, name: String, age: Int) {}

}

