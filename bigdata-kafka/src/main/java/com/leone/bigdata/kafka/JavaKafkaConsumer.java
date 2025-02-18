package com.leone.bigdata.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-26
 **/
public class JavaKafkaConsumer {

    private static Logger logger = LoggerFactory.getLogger(JavaKafkaConsumer.class);

    private static Producer<String, String> producer;

    private final static List<String> TOPICS = Arrays.asList("topic-test-a","topic-test-b");

    private static final String ZOOKEEPER_HOST = "node-2:2181,node-3:2181,node-4:2181";

    private static final String KAFKA_BROKER = "node-2:9092,node-3:9092,node-4:9092";

    private static Properties properties;

    static {
        properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_BROKER);
        properties.put("group.id", "test");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
    }

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(TOPICS, new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {

            }
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                // 将偏移设置到最开始
                consumer.seekToBeginning(collection);
            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                logger.info("offset: {}, key: {}, value: {} partition: {}", record.offset(), record.key(), record.value(), record.partition());
            }
        }
    }

}
