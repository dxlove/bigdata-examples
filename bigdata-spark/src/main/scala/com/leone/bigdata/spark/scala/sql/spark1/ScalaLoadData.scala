package com.leone.bigdata.spark.scala.sql.spark1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>file:///root/logs/csv
  *
  * @author leone
  * @since 2018-12-20
  **/
object ScalaLoadData {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("sql").setMaster("local"))
    val sqlContext = new SQLContext(sc)

    val rdd: RDD[String] = sc.textFile(args(0))

    //val dataFrame: DataFrame[String] = sqlContext.read.csv(args(0))
    //val dataFrame: DataFrame[String] = sqlContext.read.json(args(0))
    //val dataFrame: DataFrame[String] = sqlContext.read.parquet(args(0))

    val rowRDD: RDD[User] = rdd.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val createTime = fields(3)
      val password = fields(4)
      val description = fields(5)
      val deleted = fields(6).toBoolean
      User(id, name, age, createTime, password, description, deleted)
    })

    import sqlContext.implicits._
    val dataFrame = rowRDD.toDF

    dataFrame.registerTempTable("t_user")
    val frame: DataFrame = sqlContext.sql("select * from t_user order by id desc, age asc limit 100")
    frame.show()
    sc.stop()
  }

  case class User(id: Long, name: String, age: Int, createTime: String, password: String, description: String, deleted: Boolean)

}

