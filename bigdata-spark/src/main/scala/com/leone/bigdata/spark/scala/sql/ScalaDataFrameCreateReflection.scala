package com.leone.bigdata.spark.scala.sql

import com.leone.bigdata.spark.scala.caseclass.User
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

/**
  * <p>
  *
  * @author leone
  * @since 2019-01-10
  **/
object ScalaDataFrameCreateReflection {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("dataFrame").master("local[*]").getOrCreate()

    import spark.implicits._
    val stringRDD = spark.sparkContext.textFile(args(0))

    val dataFrame = stringRDD.map(_.split(",")).map(s => {
      User(s(0).toLong, s(1), s(2).toInt, s(3).toInt, s(4).toDouble, s(5), s(6).toBoolean)
    }).toDF()

//    val userRDD: RDD[User] = stringRDD.map(_.split(",")).map(s => {
//      User(s(0).toLong, s(1), s(2).toInt, s(3).toInt, s(4).toDouble, s(5), s(6).toBoolean)
//    })
//
//    val dataFrame: DataFrame = spark.createDataFrame(userRDD)

    dataFrame.printSchema()
    dataFrame.show()
    dataFrame.createTempView("t_user")

    spark.sql("select * from t_user where age > 20 order by age desc limit 10").show()

    dataFrame.selectExpr("userId", "username").show()
    dataFrame.where("age > 20").show()
    dataFrame.agg(sum("age"), sum("userId")).show()

    // dataFrame 转换为 rdd
    val rdd = dataFrame.rdd
    rdd.foreach(println)

    spark.stop()
  }
}
