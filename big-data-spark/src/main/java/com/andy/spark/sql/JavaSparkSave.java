package com.andy.spark.sql;

import org.apache.spark.sql.*;

import java.util.Properties;

/**
 * <p> spark sql save examples
 *
 * @author leone
 * @since 2019-03-20
 **/
public class JavaSparkSave {


    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("save").getOrCreate();

        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "root");

        Dataset<Row> dataset = spark.read().format("parquet").load("file:///root/logs/parquet/");

        dataset.createTempView("t_user");

        dataset.printSchema();

        // spark.sql("select * from t_user where user_id < 100000 order by user_id desc").show(30);

        // spark.sql("select user_id, count(user_id) count from t_user group by user_id having count > 1").show();
        // spark.sql("select count(1) as count from t_user").show();

        // spark.sql("select * from t_user where deleted = false and age < 18 and create_time < '2015-01-01 00:00:00' order by user_id").show();

        dataset.write().mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/db01?useSSL=false","t_user_bak", props);
        dataset.write().mode(SaveMode.Overwrite).json("file:///root/output/json/");
        dataset.write().mode(SaveMode.ErrorIfExists).csv("file:///root/output/csv/");
        dataset.write().format("parquet").mode(SaveMode.Ignore).save("file:///root/output/parquet/");

        spark.stop();
    }

}
