package com.leone.bigdata.spark.scala.examples.monitor

import java.text.SimpleDateFormat

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * <p>
  *
  * @author leone
  * @since 2019-06-14
  **/
object MonitorUtils {

  val dateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
  val jedisPool = new JedisPool(new GenericObjectPoolConfig, "localhost", 6379, 1000, "", 0)

  /**
    * 计算时差
    *
    * @param startTime
    * @param endTime
    * @return
    */
  def calculateTime(startTime: String, endTime: String): Long = {
    val st = dateFormat.parse(startTime.substring(0, 17)).getTime
    val et = dateFormat.parse(endTime).getTime
    et - st
  }

  /**
    * 获取redis连接
    *
    * @return
    */
  def getJedisClient(): Jedis = {
    jedisPool.getResource
  }

}
