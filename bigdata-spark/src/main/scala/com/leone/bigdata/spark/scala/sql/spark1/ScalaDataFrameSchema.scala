package com.leone.bigdata.spark.scala.sql.spark1

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-20
  **/
object ScalaDataFrameSchema {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("sql").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val lines = sc.textFile("hdfs://node-1:9000/spark/input4")

    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = line(3).toDouble
      println(id, name, age, fv)
      Row(id, name, age, fv)
    })

    val schema = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))

    val df = sqlContext.createDataFrame(rowRDD, schema)
    df.registerTempTable("t_person")
    sqlContext.sql("select * from t_person order by fv desc, age asc").show()

    import sqlContext.implicits._
    val df1 = df.select("name", "age", "fv")
    df1.orderBy($"fv" desc, $"age" asc).show()

    sc.stop()
  }

}
