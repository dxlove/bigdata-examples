package com.leone.bigdata.spark.java.core.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * <p> spark rdd checkpoint 机制
 *
 * @author leone
 * @since 2019-02-17
 **/
public class JavaSparkRddCheckpoint {

    public static void main(String[] args) {
        try {
            JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("spark-checkpoint").setMaster("local[*]"));
            // 设置 checkpoint 目录
            sparkContext.setCheckpointDir("file:///tmp/output/");

            JavaRDD<String> rdd = sparkContext.textFile("file:///tmp/input/hello.txt");

            // 注意： 现需要对RDD进行缓存
            JavaPairRDD<String, Integer> javaPairRDD = rdd.flatMapToPair((PairFlatMapFunction<String, String, Integer>) s -> {
                String[] arrays = s.split(" ");
                List<Tuple2<String, Integer>> list = new ArrayList<>(arrays.length);
                for (String arr : arrays) {
                    list.add(new Tuple2<>(arr, 1));
                }
                return list.iterator();
            }).cache();

            // 为pairRDD设置检查点
            javaPairRDD.checkpoint();

            System.err.println("isCheckpointed:" + javaPairRDD.isCheckpointed() + " -- checkpoint:" + javaPairRDD.getCheckpointFile());

            javaPairRDD.collect();

            System.err.println("isCheckpointed:" + javaPairRDD.isCheckpointed() + " -- checkpoint:" + javaPairRDD.getCheckpointFile());

            sparkContext.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}