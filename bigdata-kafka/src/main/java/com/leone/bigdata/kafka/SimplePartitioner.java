package com.leone.bigdata.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

/**
 * <p> 自定义分区规则
 *
 * @author leone
 * @since 2019-06-15
 **/
public class SimplePartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        List<PartitionInfo> partitionInfoList = cluster.partitionsForTopic(topic);
        int num = partitionInfoList.size();
        int partNum = key.hashCode();
        return Math.abs(partNum % num);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }

}

