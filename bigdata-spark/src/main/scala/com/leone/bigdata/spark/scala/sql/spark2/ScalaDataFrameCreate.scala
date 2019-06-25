package com.leone.bigdata.spark.scala.sql.spark2

import com.leone.bigdata.spark.scala.caseclass.User
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * <p>
  *
  * @author leone
  * @since 2019-01-10
  **/
object ScalaDataFrameCreate {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("dataFrame").master("local[*]").getOrCreate()

    val stringRDD = spark.sparkContext.textFile(args(0))

    val userRDD: RDD[User] = stringRDD.map(_.split(",")).map(s => {
      User(s(0).toLong, s(1), s(2).toInt, s(3).toInt, s(4).toDouble, s(5), s(6).toBoolean)
    })

    val dataFrame: DataFrame = spark.createDataFrame(userRDD)

    dataFrame.printSchema()
    dataFrame.show()
    dataFrame.createTempView("t_user")

    spark.sql("select * from t_user where age > 20 order by age desc limit 10").show()

    dataFrame.selectExpr("userId", "username").show()
    dataFrame.where("age > 20").show()
    dataFrame.agg(sum("age"), sum("userId")).show()

    spark.stop()
  }
}

