package com.andy.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-20
 **/
public class GenericLoadSave {

    public static void main(String[] args) throws AnalysisException {

        // 创建sparkContext
        SparkConf conf = new SparkConf().setAppName("rdd").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        Dataset<Row> dataset = sqlContext.read().load("file:///D:/root/logs/parquet/");

        dataset.createTempView("t_user");

//        sqlContext.sql("select * from t_user where user_id < 100000 order by user_id desc").show(50);
//        sqlContext.sql("select user_id, count(user_id) count from t_user group by user_id having count > 1").show();
//        sqlContext.sql("select count(1) from t_user").show();
        sqlContext.sql("select * from t_user where deleted = false and age < 18 and create_time < '2015-01-01 00:00:00' order by user_id").show();

        dataset.printSchema();

        // dataset.show();
    }

}
