package com.leone.bigdata.spark.java.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
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
public class JavaSparkLoadParquet {

    public static void main(String[] args) {
        // 创建 sparkSql 上下文
        SparkSession spark = SparkSession.builder().appName("parquet").config("spark.master", "local[*]").getOrCreate();
        Dataset<Row> parquet = spark.read().parquet(args[0]);

        parquet.registerTempTable("t_user");
        spark.sql("select * from t_user where age < 34").show();

        JavaRDD<Row> rowJavaRDD = parquet.javaRDD();
        JavaRDD<String> map = rowJavaRDD.map((Function<Row, String>) row -> "name" + row.getString(1));

        List<String> collect = map.collect();

        collect.forEach(System.out::println);

        spark.stop();
    }

}
