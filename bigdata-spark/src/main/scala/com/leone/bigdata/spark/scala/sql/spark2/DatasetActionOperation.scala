package com.leone.bigdata.spark.scala.sql.spark2

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-04-01
  **/
object DatasetActionOperation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DatasetActionOperation").master("local[*]").getOrCreate()

    val dataFrame = spark.read.format("json").load("file:///root/logs/json")

    // 将集群中的数据获取到driver端
    dataFrame.collect().foreach(println)

    // 对dataset中的数据进行统计个数操作
    println(dataFrame.count())

    // 获取数据集中的第一条数据
    println(dataFrame.first())

    //
    dataFrame.foreach(println(_))

    // 对数据集中的所有数据进行规约操作多条变一条
    // dataFrame.map(user => 1).reduce(_ + _)

    dataFrame.show()

    dataFrame.take(3).foreach(println)

    spark.close()

  }

}
