package com.leone.bigdata.storm.wc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Objects;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-26
 **/
public class WcTopologyMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException, InterruptedException {

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("wc-spout", new WcSpout(), 3).setNumTasks(2);
        topologyBuilder.setBolt("split-bolt", new WcSplitBolt(), 4).shuffleGrouping("wc-spout").setNumTasks(3);
        topologyBuilder.setBolt("count-bolt", new WcCountBolt(), 5).fieldsGrouping("split-bolt", new Fields("word")).setNumTasks(4);
        Config config = new Config();
        config.setDebug(true);

        // storm的运行模式有两种：本地模式和分布式模式
        if (Objects.nonNull(args) && args.length > 0) {
            config.setNumWorkers(3);
            // [集群模式] 注意后面一定要跟上任务的名称
            // $STORM_HOME/bin/storm jar xxx.jar com.xxx.xxx.log.TopologyMain call-log
            StormSubmitter.submitTopologyWithProgressBar(args[0], config, topologyBuilder.createTopology());
        } else {
            // [本地模式]
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology("wc", config, topologyBuilder.createTopology());
            config.setMaxTaskParallelism(3);
            Thread.sleep(10000);
            localCluster.shutdown();
        }
    }

}
