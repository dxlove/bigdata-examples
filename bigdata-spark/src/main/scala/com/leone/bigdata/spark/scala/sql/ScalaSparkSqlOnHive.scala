package com.leone.bigdata.spark.scala.sql

import org.apache.spark.sql.{Row, SparkSession}

import scala.util.Random

/**
  * <p> spark on hive 需要在classpath下加上hive-site.xml文件
  * 如果遇到权限问题可以在VM参数上加这个参数  -DHADOOP_USER_NAME=root
  *
  * @author leone
  * @since 2019-03-31
  **/
object ScalaSparkSqlOnHive {

  case class Score(id: Int, studentId: Int, score: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSql2xOnHive")
      .master("local[*]")
      // 启用hive支持
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

//    spark.sql("drop table if exists test.t_student")
//    spark.sql("create table if not exists test.t_student(id long, name string, age int) row format delimited fields terminated by ','")
//    spark.sql("load data local inpath 'file:///root/data/student.txt' into table test.t_student")

    val dataFrame = spark.sql("select * from test.t_student where age < 30 order by age desc")
    dataFrame.show()


    // dataFrame转dataset
    val dataset = dataFrame.map {
      case Row(id: Int, name: String, age: Int) => s"id: $id, name: $name, age: $age"
    }.show()

    val random = Random
    spark.createDataFrame((1 to 100).map(i => Score(i, random.nextInt(13), random.nextInt(100)))).createTempView("tv_score")

    spark.sql("select * from test.t_student st inner join tv_score sc on st.id = sc.studentId where age < 30 order by age desc").show()

    spark.stop()
  }

}
