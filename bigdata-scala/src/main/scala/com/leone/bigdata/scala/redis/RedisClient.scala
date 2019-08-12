package com.leone.bigdata.scala.redis

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.params.SetParams
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
    init("ip", 6379, 1000, "1DF2D35543FE", 2)
    val map = new java.util.HashMap[String, String]
    map.put("name", "1")
    map.put("age", "2")
    map.put("me", "3")

    set("leone", map)

    println(hGet("leone"))

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
    * 根据 key 获取 value
    *
    * @param key
    * @return
    */
  def hGet(key: String): java.util.Map[String, String] = {
    val jedis = getResource
    val result = jedis.hgetAll(key)
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
    * @return OK
    */
  def set(key: String, value: String, unixTime: Int): String = {
    val jedis = getResource
    // NX是不存在时才set， XX是存在时才set， EX是秒，PX是毫秒
    val result = jedis.set(key, value, SetParams.setParams().ex(unixTime))
    returnResource(jedis)
    result
  }

  /**
    * 设置hash类型
    *
    * @param key
    * @param value
    * @return
    */
  def set(key: String, value: java.util.Map[String, String]): Long = {
    val jedis = getResource
    val long = jedis.hset(key, value)
    returnResource(jedis)
    long
  }

  /**
    * 设置hash类型并设置过期时间
    *
    * @param key
    * @param value
    * @param unixTime
    * @return
    */
  def set(key: String, value: java.util.Map[String, String], unixTime: Int): Long = {
    val jedis = getResource
    val long = jedis.hset(key, value)
    jedis.expireAt(key, unixTime)
    returnResource(jedis)
    long
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
