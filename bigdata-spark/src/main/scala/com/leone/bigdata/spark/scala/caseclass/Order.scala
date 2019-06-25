package com.leone.bigdata.spark.scala.caseclass

/**
  * <p>
  *
  * @author leone
  * @since 2019-06-25
  **/
case class Order(orderId: Long, userId: Long, productName: String, productCount: Int, totalAmount: Double, productPrice: Double, createTime: String)
