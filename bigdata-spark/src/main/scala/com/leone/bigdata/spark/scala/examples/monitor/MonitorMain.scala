package com.leone.bigdata.spark.scala.examples.monitor

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
  * <p> spark 计算指标
  *
  * @author leone
  * @since 2019-06-14
  **/
object MonitorMain {
  // 屏蔽日志
  Logger.getLogger("org.apache").setLevel(Level.WARN)

  def main(args: Array[String]): Unit = {
    val load = ConfigFactory.load()

    // 创建kafka相关参数
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> load.getString("kafka.bootstrapServers"),
      ConsumerConfig.GROUP_ID_CONFIG -> load.getString("kafka.groupId"),
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> "20000",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val topics = load.getString("kafka.topics").split(",").toSet

    // StreamingContext
    val sparkConf = new SparkConf().setAppName("Real-time")/*.setMaster("local[*]")*/

    // 批次时间应该大于这个批次处理完的总的花费（total delay）时间
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    DBs.setup()
    // 从 mysql 中读取 kafka 偏移量信息映射到 TopicPartition ---- 从数据库中获取到当前的消费到的偏移量位置 -- 从该位置接着往后消费
    val offSetMap: Map[TopicPartition, Long] = DB.readOnly(implicit session => {
      SQL("select * from t_offset where group_id = ?").bind(load.getString("kafka.groupId")).map(rs => {
        (new TopicPartition(rs.string("topic"), rs.int("partition")), rs.long("offset"))
      }).list().apply()
    }).toMap

    val stream = if (offSetMap.isEmpty) {
      // 程序第一次启动
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    } else {
      /*var checkedOffset = Map[TopicPartition, Long]()
      val consumer = new KafkaConsumer[String, String](kafkaParams.asJava)
      consumer.subscribe(topics.asJava)
      val earliestLeaderOffsets = kafkaCluster.getEarliestLeaderOffsets(offSetMap.keySet)
      if (earliestLeaderOffsets.isRight) {
        val topicAndPartitionToOffset = earliestLeaderOffsets.right.get
        // 开始对比
        checkedOffset = offSetMap.map(owner => {
          val clusterEarliestOffset = topicAndPartitionToOffset.get(owner._1).get.offset
          if (owner._2 >= clusterEarliestOffset) {
            owner
          } else {
            (owner._1, clusterEarliestOffset)
          }
        })
      }*/
      // 程序非第一次启动
      //val messageHandler = (mm: MessageAndMetadata[String, String]) => (mm.key(), mm.message())
      KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offSetMap))
    }

    //    /**
    //      * receiver 接受数据是在Executor端 cache -- 如果使用的窗口函数的话，没必要进行cache, 默认就是cache， WAL ；
    //      * 如果采用的不是窗口函数操作的话，你可以cache, 数据会放做一个副本放到另外一台节点上做容错
    //      * direct 接受数据是在Driver端
    //      */
    //    // 处理数据 ---- 根据业务--需求
    //    stream.foreachRDD(rdd => {
    //      // rdd.foreach(println)
    //      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    //      val baseData = rdd.map(t => JSON.parseObject(t._2))
    //        .filter(_.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq"))
    //        .map(jsObj => {
    //          val result = jsObj.getString("bussinessRst")
    //          val fee: Double = if (result.equals("0000")) jsObj.getDouble("chargefee") else 0
    //          val isSucc: Double = if (result.equals("0000")) 1 else 0
    //
    //          val receiveTime = jsObj.getString("receiveNotifyTime")
    //          val startTime = jsObj.getString("requestId")
    //
    //          val pCode = jsObj.getString("provinceCode")
    //          // 消耗时间
    //          val costime = if (result.equals("0000")) Utils.caculateRqt(startTime, receiveTime) else 0
    //          ("A-" + startTime.substring(0, 8), startTime.substring(0, 10), List[Double](1, isSucc, fee, costime.toDouble), pCode, startTime.substring(0, 12))
    //        })
    //
    //
    //      // 实时报表 -- 业务概况
    //      /**
    //        * 1)统计全网的充值订单量, 充值金额, 充值成功率及充值平均时长.
    //        */
    //      baseData.map(t => (t._1, t._3)).reduceByKey((list1, list2) => {
    //        (list1 zip list2) map (x => x._1 + x._2)
    //      }).foreachPartition(itr => {
    //
    //        val client = JedisUtils.getJedisClient()
    //
    //        itr.foreach(tp => {
    //          client.hincrBy(tp._1, "total", tp._2(0).toLong)
    //          client.hincrBy(tp._1, "succ", tp._2(1).toLong)
    //          client.hincrByFloat(tp._1, "money", tp._2(2))
    //          client.hincrBy(tp._1, "timer", tp._2(3).toLong)
    //
    //          client.expire(tp._1, 60 * 60 * 24 * 2)
    //        })
    //        client.close()
    //      })
    //
    //      // 每个小时的数据分布情况统计
    //      baseData.map(t => ("B-" + t._2, t._3)).reduceByKey((list1, list2) => {
    //        (list1 zip list2) map (x => x._1 + x._2)
    //      }).foreachPartition(itr => {
    //
    //        val client = JedisUtils.getJedisClient()
    //
    //        itr.foreach(tp => {
    //          // B-2017111816
    //          client.hincrBy(tp._1, "total", tp._2(0).toLong)
    //          client.hincrBy(tp._1, "succ", tp._2(1).toLong)
    //
    //          client.expire(tp._1, 60 * 60 * 24 * 2)
    //        })
    //        client.close()
    //      })
    //
    //
    //
    //      // 每个省份充值成功数据
    //      baseData.map(t => ((t._2, t._4), t._3)).reduceByKey((list1, list2) => {
    //        (list1 zip list2) map (x => x._1 + x._2)
    //      }).foreachPartition(itr => {
    //
    //        val client = JedisUtils.getJedisClient()
    //
    //        itr.foreach(tp => {
    //          client.hincrBy("P-" + tp._1._1.substring(0, 8), tp._1._2, tp._2(1).toLong)
    //          client.expire("P-" + tp._1._1.substring(0, 8), 60 * 60 * 24 * 2)
    //        })
    //        client.close()
    //      })
    //
    //
    //      // 每分钟的数据分布情况统计
    //      baseData.map(t => ("C-" + t._5, t._3)).reduceByKey((list1, list2) => {
    //        (list1 zip list2) map (x => x._1 + x._2)
    //      }).foreachPartition(itr => {
    //
    //        val client = JedisUtils.getJedisClient()
    //
    //        itr.foreach(tp => {
    //          client.hincrBy(tp._1, "succ", tp._2(1).toLong)
    //          client.hincrByFloat(tp._1, "money", tp._2(2))
    //          client.expire(tp._1, 60 * 60 * 24 * 2)
    //        })
    //        client.close()
    //      })
    //  )

    stream.foreachRDD({ rdd =>
      //数据处理
      val result: RDD[(String, Int)] = rdd.flatMap(_.value().split(" ")).map((_, 1)).reduceByKey(_ + _)
      result.foreach(println)
      result.foreachPartition({ it =>
        val jedis = MonitorUtils.getJedisClient()
        it.foreach({ va =>
          jedis.hincrBy("wc", va._1, va._2)
        })
        jedis.close()
      })

      // 记录偏移量存入 mysql
      val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      DB.localTx { implicit session =>
        for (or <- offsetRanges) {
          SQL("REPLACE INTO `t_offset` (`group_id`, `topic`, `partition`, `offset`) VALUES (?, ?, ?, ?)")
            .bind(load.getString("kafka.groupId"), or.topic, or.partition, or.untilOffset).update().apply()
        }
      }
    })
    // 启动程序，等待程序终止
    ssc.start()
    ssc.awaitTermination()
  }
}