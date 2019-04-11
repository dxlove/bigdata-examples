package com.andy.spark.sql;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-21
 **/
public class HiveDataSource {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("local[*]")
                //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                //.config("hadoop.home.dir", "/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("drop if exists table t_student");

        spark.sql("create table if exists t_student(id long, name string, age int)");

        spark.sql("load data local inpath 'file:///root/input/student.txt' into table t_student");

        spark.sql("drop table if exists t_sc");

        spark.sql("create if not exists table t_sc(id long, student_id int, score int)");

        spark.sql("load data local inpath 'file:///root/input/sc.txt' into table t_sc");

        Dataset<Row> sql = spark.sql("select * from t_sc sc left join t_student s where s.id = sc.student_id");

        sql.createTempView("t_temp");

        spark.sql("select * from t_temp");

        spark.stop();
    }

}
