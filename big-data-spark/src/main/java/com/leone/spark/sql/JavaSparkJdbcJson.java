package com.leone.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * <p>
 *
 * @author leone
 * @since 2019-01-10
 **/
public class JavaSparkJdbcJson {

    public static void main(String[] args) {
        // 创建 spark 统一入口
        SparkSession spark = SparkSession.builder()
                .appName("json")
                .config("spark.master", "local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("file:///root/logs/json/");
        df.show(3);

        df.createOrReplaceTempView("t_user");

        spark.sql("select * from t_user where age > 22").show();

        // df.where("age > 23").show();
        // spark.sql("select count(1) from customer").show();

        JavaRDD<Row> javaRDD = df.toJavaRDD();

        // dataFrame 和 RDD 转换
        // javaRDD.foreach(e -> System.out.println(e.getLong(0) + "\t" + e.getLong(1) + "\t" + e.getString(2)));

        df.write().mode(SaveMode.Append).json("file:///root/output/json/");
        // df.write().json("file:///root/output/json/user.json");

    }

}
