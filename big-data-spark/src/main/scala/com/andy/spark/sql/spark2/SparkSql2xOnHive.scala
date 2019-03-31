package com.andy.spark.sql.spark2

import org.apache.spark.sql.{Row, SparkSession}

/**
  * <p>
  * 如果遇到权限问题可以在VM参数上加这个参数  -DHADOOP_USER_NAME=root
  *
  * @author leone
  * @since 2019-03-31
  **/
object SparkSql2xOnHive {

  case class Record(key: Int, value: String)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkSql2xOnHive")
      .master("local[*]")
      // 启用hive支持
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    spark.sql("drop table t_record")
    spark.sql("create table if not exists t_record(key int, value string)")
    spark.sql("load data local inpath 'file:///d:/root/logs/kv/kv.txt' into table t_record")

    val sqlHiveDF = spark.sql("select * from t_record where key < 10 order by key")

    val sqlHiveDS = sqlHiveDF.map {
      case Row(key: Int, value: String) => s"KEY: $key, VALUE: $value"
    }

    sqlHiveDS.show()

    val recordDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordDF.createTempView("record")
    spark.sql("select * from record join t_record on t_record.key = record.key").show()

    spark.stop()
  }

}
