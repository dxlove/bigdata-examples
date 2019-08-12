package com.leone.bigdata.kafka;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-26
 **/
public class JavaKafkaProducer {

    private static Logger logger = LoggerFactory.getLogger(JavaKafkaProducer.class);

    private static Producer<String, String> producer;

    private final static String TOPIC = "topic-test-b";

    private static final String ZOOKEEPER_HOST = "node-2:2181,node-3:2181,node-4:2181";

    private static final String KAFKA_BROKER = "node-2:9092,node-3:9092,node-4:9092";

    private static Map<String, Object> paramsMap;

    static {
        paramsMap = new HashMap<>();
        paramsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        paramsMap.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        paramsMap.put("key.serializer", StringSerializer.class.getName());
        paramsMap.put("value.serializer", StringSerializer.class.getName());
        paramsMap.put("partitioner.class", SimplePartitioner.class.getName());
        System.out.println(paramsMap);

    }

    public static void main(String[] args) {
        // 生产者对象
        Producer<String, String> producer = new KafkaProducer<>(paramsMap);
        for (int i = 2000000; i < Integer.MAX_VALUE; i++) {
            //try {
            //Thread.sleep(1000);
            //} catch (InterruptedException e) {
            //    e.printStackTrace();
            //}
            String uuid = UUID.randomUUID().toString();
            producer.send(new ProducerRecord<>(TOPIC, Integer.toString(i), uuid));
            logger.info("send key: {} value: {} partition: {}", i, uuid, Integer.toString(i).hashCode() % 3);
        }
        producer.close();
    }

}
