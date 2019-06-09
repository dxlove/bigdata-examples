package com.leone.bigdata.spark.java.core.examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * <p>
 *
 * @author leone
 * @since 2018-12-20
 **/
public class TopExample {

    public static void main(String[] args) {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("top").setMaster("local"));

        JavaRDD<String> wordsRDD = sc.textFile("d:/root/input/user.log");


        sc.close();

    }

}
