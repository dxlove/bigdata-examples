package com.leone.bigdata.spark.scala.examples

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2019-07-13
  **/
object TopN {

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("topN"))

    val rdd = sc.textFile(args(0))

    println(rdd.count())

    sc.stop()
  }

}
