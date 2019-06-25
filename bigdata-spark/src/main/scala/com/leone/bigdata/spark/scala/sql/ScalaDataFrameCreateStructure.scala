package com.leone.bigdata.spark.scala.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-20
  **/
object ScalaDataFrameCreateStructure {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("dataFrame").master("local[*]").getOrCreate()

    val stringRdd: RDD[String] = spark.sparkContext.textFile(args(0))

    val rowRDD: RDD[Row] = stringRdd.map(line => {
      val fields = line.split(",")
      Row(fields(0).toLong, fields(1), fields(2).toInt, fields(3).toInt, fields(4).toDouble, fields(5), fields(6).toBoolean)
    })

    // 结果类型，用来约束 DataFrame
    val schema: StructType = StructType(List(
      StructField("userId", LongType, true),
      StructField("username", StringType, true),
      StructField("sex", IntegerType, true),
      StructField("age", IntegerType, true),
      StructField("credit", DoubleType, true),
      StructField("createTime", StringType, true),
      StructField("deleted", BooleanType, true)
    ))

    val dataFrame: DataFrame = spark.createDataFrame(rowRDD, schema)

    // 导入隐式转换
    import spark.implicits._
    // 导入聚合函数
    import org.apache.spark.sql.functions._

    dataFrame.where($"credit" > 600.00).orderBy($"credit" desc, $"age" asc).show()

    dataFrame.groupBy($"age" as "age").count().sort($"count" desc).show()

    dataFrame.groupBy($"age" as "age").agg(count("*") as "counts").orderBy($"counts" desc, $"age" asc).show()

    // dataFrame 转换为 rdd
    val rdd = dataFrame.rdd
    rdd.foreach(println)

    spark.stop()
  }

}
