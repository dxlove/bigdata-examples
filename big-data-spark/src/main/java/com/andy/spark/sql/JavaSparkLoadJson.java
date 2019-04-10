package com.andy.spark.sql;

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

        SparkSession spark = SparkSession.builder().appName("javaSql").config("spark.master", "local[*]").getOrCreate();

        Dataset<Row> json = spark.read().json("file:///root/logs/json/");

        json.createTempView("t_user");

        Dataset<Row> dataset = spark.sql("select * from t_user where age < 34");

        dataset.show();

        List<Long> collect = dataset.javaRDD().map((Function<Row, Long>) row -> row.getLong(0)).collect();

        collect.forEach(System.out::println);

        spark.stop();
    }

}
