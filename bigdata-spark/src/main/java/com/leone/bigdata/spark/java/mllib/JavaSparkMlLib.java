package com.leone.bigdata.spark.java.mllib;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * <p> ML lib test 决策树
 *
 * @author leone
 * @since 2019-01-14
 **/
public class JavaSparkMlLib {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[5]").appName("DecisionTreeTest").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> lines = jsc.textFile(args[1]);
        final HashingTF tf = new HashingTF(10000);

        JavaRDD<LabeledPoint> transData = lines.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String str) throws Exception {
                String[] t1 = str.split(",");
                String[] t2 = t1[1].split(" ");
                return new LabeledPoint(Double.parseDouble(t1[0]), tf.transform(Arrays.asList(t2)));
            }
        });


        // 设置决策树参数，训练模型
        Integer numClasses = 3;
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        final DecisionTreeModel tree_model = DecisionTree.trainClassifier(transData, numClasses,
                categoricalFeaturesInfo, impurity, maxDepth, maxBins);

        System.out.println("决策树模型：" + tree_model.toDebugString());

        // 保存模型
        tree_model.save(jsc.sc(), args[2]);

        // 未处理数据，带入模型处理
        JavaRDD<String> testLines = jsc.textFile(args[3]);
        JavaPairRDD<String, String> res = testLines.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                String[] t2 = line.split(",")[1].split(" ");
                Vector v = tf.transform(Arrays.asList(t2));
                double res = tree_model.predict(v);
                return new Tuple2<>(line, Double.toString(res));
            }
        }).cache();

        // 打印结果
        res.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> a) throws Exception {
                System.out.println(a._1 + " : " + a._2);
            }
        });

        // 将结果保存在本地
        res.saveAsTextFile(args[4]);
        spark.stop();
    }


}
