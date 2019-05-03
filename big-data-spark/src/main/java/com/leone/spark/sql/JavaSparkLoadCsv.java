package com.leone.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-21
 **/
public class JavaSparkLoadCsv {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("csv").master("local[*]").getOrCreate();

        Dataset<Row> parquet = spark.read().format("csv").load("file:///Users/leone/Documents/logs/csv/");

        parquet.printSchema();

        parquet.createOrReplaceTempView("t_user");

        parquet.show();

        spark.stop();
    }

}
