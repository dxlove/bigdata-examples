package com.leone.spark.sql.spark2

import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-21
  **/
object SparkSqlJoin {

  def main(args: Array[String]): Unit = {

    // spark2.x 统一使用sparkSession作为spark的入口
    val spark = SparkSession.builder().appName("sql").master("local[*]").getOrCreate()

    import spark.implicits._
    val lines: Dataset[String] = spark.createDataset(List("1,jack,china", "2,andy,usa", "3,james,jp"))

    // 表一
    val tpDs: Dataset[(Long, String, String)] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val nationCode = fields(2)
      (id, name, nationCode)
    })

    val df1 = tpDs.toDF("user_id", "username", "country_code")

    // 表二
    val country: Dataset[String] = spark.createDataset(List("china,中国", "usa,美国"))
    val df2 = country.map(l => {
      val fields = l.split(",")
      val code = fields(0)
      val name = fields(1)
      (code, name)
    }).toDF("code", "name")

    // 表一表二关联

    //    df1.createTempView("v_user")
    //    df2.createTempView("v_nations")
    //    val r: DataFrame = spark.sql("select name,cname from v_user join v_nations on v_user.nations = v_nations.ename")
    //    r.show()

    val r = df1.join(df2, $"country_code" === $"code")

    r.show()

    spark.stop()
  }

}
