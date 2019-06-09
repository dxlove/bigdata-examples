package com.leone.bigdata.spark.java.sql;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * <p> spark jdbc 查询数据库
 *
 * @author leone
 * @since 2019-01-10
 **/
public class JavaSparkLoadJdbc {

    public static void main(String[] args) {
        // 创建 sparkSql 上下文
        SparkSession spark = SparkSession.builder().appName("javaSql").config("spark.master", "local[*]").getOrCreate();

        String url = "jdbc:mysql://ip:3306/db01?useSSL=false";
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "xxx");

//        Dataset<Row> df = spark.read()
//                .format("jdbc")
//                .option("url", "jdbc:mysql://localhost:3306/spark")
//                .option("dbtable", "t_logs")
//                .option("user", "root")
//                .option("password", "root")
//                .option("driver", "com.mysql.jdbc.Driver")
//                .load();

        Dataset<Row> df = spark.read().jdbc(url, "t_student", props);

        df.select(new Column("s_id").as("id"), new Column("s_name").as("name")).where("s_age < 22").distinct().limit(20).show();

        df.write().jdbc(url, "t_user_bak1", props);

        spark.close();
    }
}
