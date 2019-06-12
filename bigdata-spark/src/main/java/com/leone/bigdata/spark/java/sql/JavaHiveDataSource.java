package com.leone.bigdata.spark.java.sql;

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
public class JavaHiveDataSource {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Hive Example")
                .master("local[*]")
                //.config("spark.sql.warehouse.dir", "/user/hive/warehouse")
                //.config("hadoop.home.dir", "/user/hive/warehouse")
                .enableHiveSupport()
                .getOrCreate();

        spark.sql("drop table if exists test.t_student");
        spark.sql("create table if not exists test.t_student(id long, name string, age int) row format delimited fields terminated by ','");
        spark.sql("load data local inpath 'file:///root/data/student.txt' into table test.t_student");

        spark.sql("drop table if exists test.t_score");
        spark.sql("create table if not exists test.t_score(id long, student_id int, score int) row format delimited fields terminated by ','");
        spark.sql("load data local inpath 'file:///root/data/score.txt' into table test.t_score");

        Dataset<Row> sql = spark.sql("select * from test.t_score sc left join test.t_student st where st.id = sc.student_id");
        sql.createTempView("t_temp");
        spark.sql("select * from t_temp").show();

        spark.stop();
    }

}
