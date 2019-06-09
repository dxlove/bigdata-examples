package com.leone.bigdata.spark.java.core.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * <p>RDD 持久化
 *
 * @author leone
 * @since 2018-12-19
 **/
public class JavaRddPersist {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("persist").setMaster("local[*]"));

        //JavaRDD<String> javaRDD = sc.textFile("file:///root/logs/csv").cache();
        JavaRDD<String> javaRDD = sc.textFile("file:///root/logs/csv").persist(StorageLevel.MEMORY_ONLY());
        //JavaRDD<String> javaRDD = sc.textFile("file:///root/logs/csv");

        long begin = System.currentTimeMillis();
        long count = javaRDD.count();
        System.out.println(count);
        long end = System.currentTimeMillis();
        System.err.println("time: " + (end - begin) + " millisecond!");

        begin = System.currentTimeMillis();
        count = javaRDD.count();
        System.out.println(count);
        end = System.currentTimeMillis();
        System.err.println("time: " + (end - begin) + " millisecond!");

        sc.close();
    }

}
