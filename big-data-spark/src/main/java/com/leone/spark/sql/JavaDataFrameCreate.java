package com.leone.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-20
 **/
public class JavaDataFrameCreate {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("dataFrame").getOrCreate();

        Dataset<Row> json = spark.read().json("file:///root/logs/json/");

        // 等于sql中查询所有
        json.show();

        // 打印表的原数据信息
        json.printSchema();

        json.select(json.col("username"), json.col("age").plus(1)).show();

        // 根据某一列的值进行过滤
        json.filter(json.col("age").gt(18)).show();

        // 根据某一列进行分组
        json.groupBy(json.col("age")).count().orderBy(json.col("age")).describe("age").show();

        spark.stop();
    }

}
