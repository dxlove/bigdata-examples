package com.leone.bigdata.spark.java.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-21
 **/
public class JavaSparkLoadJson {

    public static void main(String[] args) throws AnalysisException {
        // 创建 spark 统一入口
        SparkSession spark = SparkSession.builder().appName("javaSql").config("spark.master", "local[*]").getOrCreate();

        Dataset<Row> json = spark.read().json("file:///root/logs/json/");

        json.createTempView("t_user");
        Dataset<Row> dataset = spark.sql("select * from t_user where age < 34");
        dataset.show();

        //df.where("age > 23").show();
        //spark.sql("select count(1) from customer").show();

        //Dataset 和 RDD 转换
        JavaRDD<Row> javaRDD = json.toJavaRDD();
        //javaRDD.foreach(e -> System.out.println(e.getLong(0) + "\t" + e.getLong(1) + "\t" + e.getString(2)));

        //json.write().mode(SaveMode.Append).json("file:///root/output/json/");
        //json.write().json("file:///root/output/json/user.bak.json");

        List<Long> collect = dataset.javaRDD().map((Function<Row, Long>) row -> row.getLong(0)).collect();
        collect.forEach(System.out::println);

        spark.stop();
    }

}
