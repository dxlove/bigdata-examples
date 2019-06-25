package com.leone.bigdata.spark.scala.sql

import org.apache.spark.sql.SparkSession

/**
  * <p>
  *
  * @author leone
  * @since 2019-04-01
  **/
object TypedOperation {

  case class User(user_id: Long, username: String, age: Long, sex: String, tel: String, create_time: String, integral: Long)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("BasicOperation").master("local[*]").getOrCreate()

    import spark.implicits._

    val dataFrame = spark.read.format("json").load("file:///root/logs/json/log-20190331-1.json")

    val userDS = dataFrame.as[User]

    val userDSRepartitioned = userDS.repartition(7)

    // 打印分区数量
    println(userDSRepartitioned.rdd.partitions.size)

    // 从新分区
    //val userDSCoalesced = userDSRepartitioned.coalesce(3)

    //println(userDSCoalesced.rdd.partitions.size)

    //userDSCoalesced.show()

    //println(userDSCoalesced.count())

    // distinct 是根据所有内容去比对去重
    //val distinct = userDSCoalesced.distinct()

    //println(distinct.count())

    // dropDuplicates 是根据某个字段去重
    //val dropDuplicates = userDSCoalesced.dropDuplicates(Seq("age"))

    //println(dropDuplicates.count())

    val userDS2 = spark.read.format("json").load("file:///root/logs/json/log-20190331-2.json").as[User]

    //except 获取当前dataset中有，但是另一个dataset中没有的元素
    // userDS.except(userDS2).show()


    // filter 根据我们自己的逻辑如果返回true那么就保留该元素否者丢弃
    // userDS2.filter("age < 18").show()


    // intersect 获取两个集合的交集
    // userDS.intersect(userDS2).show()

    // map 将数据集中的每一条数据重新映射返回一条数据
    // userDS.map(e => (e.age, e.age + 1)).show()

    // flatMap 数据集中的每条数据都可以返回多条数据
    // userDS.flatMap(e => Seq(User(e.user_id, e.username, e.age + 10, e.sex, e.tel, e.create_time, e.integral))).show()

    // mapPartitions 一次对一个partition的数据进行处理
    //    userDS.mapPartitions(user => {
    //      val result = scala.collection.mutable.ArrayBuffer[(String, Long)]()
    //      while (user.hasNext) {
    //        val u = user.next()
    //        result += ((u.username, u.user_id))
    //      }
    //      result.iterator
    //    }).show()


    // dataset join 操作
    // userDS.joinWith(userDS2,$"user_id" === $"user_id").show()

    // data sort
    // userDS.sort($"age".desc).show()

    // val userDSArr = userDS.randomSplit(Array(3, 10, 30))
    // userDSArr.foreach(ds => ds.show())

    // 抽样
    userDS.sample(false, 0.3).show()

    spark.stop()
  }

}
