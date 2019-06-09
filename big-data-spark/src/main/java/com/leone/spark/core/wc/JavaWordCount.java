package com.leone.spark.core.wc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * java版本 word count 统计单词出现次数
 *
 * @author leone
 * @since 2018-06-10
 **/
public class JavaWordCount {

    public static void main(String[] args) {
        // 创建spark配置
        SparkConf sparkConf = new SparkConf().setAppName("word-count").setMaster("local[*]");

        // 创建sparkContext
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // 创建 初始化RDD lines 每一行元素是一行文本
        JavaRDD<String> javaRDD = sparkContext.textFile(args[0]);

        // 切分每一行
        JavaRDD<String> flatMapRDD = javaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(",")).iterator();
            }
        });

        // 映射为元组
        JavaPairRDD<String, Integer> javaPairRDD = flatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        // 根据key聚合
        JavaPairRDD<String, Integer> reduceByKeyRDD = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return Integer.sum(v1, v2);
            }
        });

        // 对key进行排序
        JavaPairRDD<String, Integer> sortByKeyRDD = reduceByKeyRDD.sortByKey();

        // 保存到文件
        sortByKeyRDD.saveAsTextFile(args[1]);

        // 关闭sparkContext
        sparkContext.close();
    }

}
