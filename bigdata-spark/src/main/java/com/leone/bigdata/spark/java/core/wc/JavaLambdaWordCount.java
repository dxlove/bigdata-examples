package com.leone.bigdata.spark.java.core.wc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * <p> java lambda 版本的 word count
 *
 * @author leone
 * @since 2018-12-02
 **/
public class JavaLambdaWordCount {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("java-lambda-word-count").setMaster("local[*]");

        // 创建 sparkContext
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // 指定从哪里读取数据
        JavaRDD<String> javaRDD = sparkContext.textFile(args[0]);

        // 切分压平
        JavaRDD<String> flatMapJavaRDD = javaRDD.flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(",")).iterator());

        // 将单词组合在一起
        JavaPairRDD<String, Integer> pairJavaRDD = flatMapJavaRDD.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

        // 聚合
        JavaPairRDD<String, Integer> reduceByKeyRDD = pairJavaRDD.reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum);

        // 交换
        JavaPairRDD<Integer, String> swapRDD = reduceByKeyRDD.mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) Tuple2::swap);

        // 排序
        JavaPairRDD<Integer, String> sortedRDD = swapRDD.sortByKey(false);

        // 调整顺序
        JavaPairRDD<String, Integer> result = sortedRDD.mapToPair((PairFunction<Tuple2<Integer, String>, String, Integer>) Tuple2::swap);

        // 将数据保存到指定位置中
        result.saveAsTextFile(args[1]);

        // 关闭 sparkContext
        sparkContext.stop();
    }

}
