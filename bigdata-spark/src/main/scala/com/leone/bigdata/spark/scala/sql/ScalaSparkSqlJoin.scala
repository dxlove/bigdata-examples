package com.leone.bigdata.spark.scala.sql

import com.leone.bigdata.spark.scala.caseclass.{Order, User}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-21
  **/
object ScalaSparkSqlJoin {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("join").master("local[*]").getOrCreate()
    // 导入隐式转换
    import spark.implicits._

    // 表一
    val userStringRDD = spark.sparkContext.textFile(args(0))
    val userRDD = userStringRDD.map(_.split(",")).map(e => {
      User(e(0).toLong, e(1), e(2).toInt, e(3).toInt, e(4).toDouble, e(5), e(6).toBoolean)
    })
    val userDataFrame = spark.createDataFrame(userRDD)

    // 表二
    val orderStringRDD = spark.sparkContext.textFile(args(1))
    val orderRDD = orderStringRDD.map(_.split(",")).map(e => {
      Row(e(0).toLong, e(1).toLong, e(2), e(3).toInt, e(4).toDouble, e(4).toDouble, e(6))
    })

    val schema = StructType(List(
      StructField("orderId", LongType, true),
      StructField("userId", LongType, true),
      StructField("productName", StringType, true),
      StructField("productCount", IntegerType, true),
      StructField("productPrice", DoubleType, true),
      StructField("totalAmount", DoubleType, true),
      StructField("createTime", StringType, true)
    ))

    val orderDataFrame = spark.createDataFrame(orderRDD, schema)

    // 表一表二关联
    userDataFrame.createTempView("tv_user")
    orderDataFrame.createTempView("tv_order")

    //spark.sql("select u.userId, u.username, o.productName, o.totalAmount from tv_user u left join tv_order o on u.userId = o.userId").show()

    userDataFrame.join(orderDataFrame, userDataFrame("userId") === orderDataFrame("orderId"), "left")
      .select(userDataFrame("userId"), userDataFrame("username"), orderDataFrame("productName"), orderDataFrame("totalAmount")).show(15)

    spark.stop()
  }

}
