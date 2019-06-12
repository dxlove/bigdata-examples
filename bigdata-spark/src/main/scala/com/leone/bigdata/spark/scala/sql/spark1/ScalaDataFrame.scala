package com.leone.bigdata.spark.scala.sql.spark1

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>file:///root/logs/json
  *
  * @author leone
  * @since 2019-03-20
  **/
object ScalaDataFrame {

  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext(new SparkConf().setAppName("dataFrame").setMaster("local[*]"))
    val sqlContext = new SQLContext(sparkContext)

    val df = sqlContext.read.json(args(0))

    df.show()

    df.printSchema()

    df.select("username").show()

    df.select(df.col("username"), df.col("age")).show()

    df.filter(df.col("age") > 18).show()

    df.groupBy("age").count().orderBy("age")

    sparkContext.stop()
  }

}
