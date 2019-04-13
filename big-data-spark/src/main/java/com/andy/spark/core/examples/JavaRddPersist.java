package com.andy.spark.core.examples;

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

        JavaRDD<String> javaRdd = sc.textFile("d:/root/logs/json").cache();
        // avaRDD<String> stringJavaRDD = sc.textFile("d:/root/logs/json").persist(StorageLevel.getCachedStorageLevel());
        // JavaRDD<String> stringJavaRDD = sc.textFile("d:/root/logs/json");

        long begin = System.currentTimeMillis();

        long count = javaRdd.count();
        System.out.println(count);
        long end = System.currentTimeMillis();

        System.err.println("time: " + (end - begin) + " millisecond!");

        begin = System.currentTimeMillis();
        count = javaRdd.count();

        System.out.println(count);
        end = System.currentTimeMillis();
        System.err.println("time: " + (end - begin) + " millisecond!");

        sc.close();
    }

}
