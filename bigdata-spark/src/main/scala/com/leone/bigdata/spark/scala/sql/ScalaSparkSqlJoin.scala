package com.leone.bigdata.spark.scala.sql

import com.leone.bigdata.spark.scala.caseclass.{Order, User}
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
    val orderStringRDD = spark.sparkContext.textFile(args(2))
    val orderRDD = orderStringRDD.map(_.split(",")).map(e => {
      (e(0).toLong, e(1).toLong, e(2), e(3).toInt, e(4).toDouble, e(4).toDouble, e(6))
    })
    val orderDataFrame = orderRDD.toDF("orderId", "userId", "productName", "productCount", "productPrice", "totalPrice")

    // 表一表二关联
    userDataFrame.createTempView("tv_user")
    orderDataFrame.createTempView("tv_order")

    spark.sql("select userId, username, productName, totalAmount from tv_user join tv_order on tv_user.userId = tv_order.userId").show()

//    val result = userDataFrame.join(orderDataFrame, $"userId" === $"userId")
//    result.select("userId", "username", "productName", "totalAmount").show()

    spark.stop()
  }

}
