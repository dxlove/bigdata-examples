package com.leone.bigdata.spark.scala.sql.spark2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-20
  **/
object ScalaDataFrameSchema {

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().appName("dataFrameSchema").master("local[*]").getOrCreate()
    val rdd: RDD[String] = session.sparkContext.textFile(args(0))

    val rowRDD: RDD[Row] = rdd.map(line => {
      val fields = line.split(",")
      Row(fields(0).toLong, fields(1), fields(2).toInt, fields(3).toDouble)
    })

    // 结果类型，表头 用户描述DataFrame
    val schema: StructType = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("face_score", DoubleType, true)
    ))

    val dataFrame: DataFrame = session.createDataFrame(rowRDD, schema)

    // 导入隐式转换
    import session.implicits._
    // 导入聚合函数
    import org.apache.spark.sql.functions._

    dataFrame.where($"face_score" > 80.00).orderBy($"face_score" desc, $"age" asc).show()

    dataFrame.groupBy($"age" as "age").count().sort($"count" desc).show()

    dataFrame.groupBy($"age" as "age").agg(count("*") as "counts").orderBy($"counts" desc, $"age" asc).show()

    session.stop()
  }

}
