package com.leone.bigdata.scala.redis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.util.Pool
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * <p>
  *
  * @author leone
  * @since 2019-06-14
  **/
object RedisClient {

  private var jedisPool: Pool[Jedis] = _

  def main(args: Array[String]): Unit = {
    init("39.108.125.41", 6379, 1000, "1DF2D35543FE", 2)
//    val str = set("hello", "hello world")
//    println(str)

//    val res = get("hello")
//    println(res)

    println(del("hello"))

  }

  /**
    * 初始化
    *
    * @param host
    * @param port
    * @param timeout
    * @param password
    * @param database
    */
  def init(host: String, port: Int, timeout: Int, password: String, database: Int = 0): Unit = {
    jedisPool = new JedisPool(new GenericObjectPoolConfig, host, port, timeout, password, database)
  }

  /**
    * 获取连接客户端
    *
    * @return
    */
  def getResource: Jedis = {
    jedisPool.getResource
    //resource.auth("1DF2D35543FE")
  }

  /**
    * 根据 key 获取 value
    *
    * @param key
    * @return
    */
  def get(key: String): String = {
    val jedis = getResource
    val result = jedis.get(key)
    returnResource(jedis)
    result
  }

  /**
    * 保存 k v
    *
    * @param key
    * @param value
    * @return
    */
  def set(key: String, value: String): String = {
    val jedis = getResource
    val result = jedis.set(key, value)
    returnResource(jedis)
    result
  }

  /**
    * 设置值并设置过期时间
    *
    * @param key
    * @param value
    * @param unixTime
    * @return
    */
  def set(key: String, value: String, unixTime: Long): String = {
    val jedis = getResource
    val result = jedis.set(key, value)
    jedis.expireAt(key, unixTime)
    returnResource(jedis)
    result
  }

  /**
    * 根据key删除
    *
    * @param key
    * @return
    */
  def del(key: String): Long = {
    val jedis = getResource
    val long = jedis.del(key)
    returnResource(jedis)
    long
  }


  /**
    * 释放资源
    *
    * @param jedis
    */
  private def returnResource(jedis: Jedis): Unit = {
    if (jedis != null) {
      jedis.close()
    }
  }


}
