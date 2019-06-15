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
  val jedisPool = new JedisPool(new GenericObjectPoolConfig, "39.108.125.41", 6379, 1000, "1DF2D35543FE", 0)

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

  /**
    * 返回连接
    *
    * @param jedis
    */
  def returnResource(jedis: Jedis): Unit = {
    jedis.close()
  }

}
