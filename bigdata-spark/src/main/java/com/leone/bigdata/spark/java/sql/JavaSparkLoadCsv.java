package com.leone.bigdata.spark.java.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * file:///Users/leone/Documents/logs/csv/
 *
 * @author leone
 * @since 2019-03-21
 **/
public class JavaSparkLoadCsv {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("csv").master("local[*]").getOrCreate();
        Dataset<Row> dataset = spark.read().format("csv").load(args[0]);

        dataset.printSchema();

        dataset.createOrReplaceTempView("t_user");

        // 显示第一条
        Row row = dataset.first();
        System.err.println(row);

        // 显示前N条
        dataset.head(5);

        //0,张盯偿,15908672349,25,3986,19770404
        // 分组
        //dataset.select("_c2").as("age").groupBy("_c2").count().as("age_count").orderBy("_c2").show();

        dataset.groupBy("_c4").max("_c5").show();

        // 默认显示20条数据
        //dataset.show();

        spark.stop();
    }

}
