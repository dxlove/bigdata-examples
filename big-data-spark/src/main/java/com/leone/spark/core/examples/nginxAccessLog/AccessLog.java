package com.leone.spark.core.examples.nginxAccessLog;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * <p>
 *
 * @author leone
 * @since 2019-04-24
 **/
public class AccessLog {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("").master("local[*]").getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("file:///root/logs/access.log");

        String url = "jdbc:mysql://localhost:3306/db01?useSSL=false";
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "root");

        Pattern p = Pattern.compile("([^ ]*) ([^ ]*) ([^ ]*) (\\[.*\\]) (\\\".*?\\\") (-|[0-9]*) (-|[0-9]*) (\\\".*?\\\") (\\\".*?\\\")([^ ]*)");


        JavaRDD<String> filtered = stringJavaRDD.filter((Function<String, Boolean>) s -> {
            Matcher matcher = p.matcher(s);
            return matcher.matches();
        });

        JavaRDD<LogBean> maped = filtered.map((Function<String, LogBean>) s -> {
            Matcher m = p.matcher(s);
            if (m.find()) {
                return new LogBean(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7), m.group(8), m.group(9), m.group(10));
            }
            return null;
        });

        Dataset<Row> dataFrame = spark.createDataFrame(maped, LogBean.class);

        dataFrame.registerTempTable("t_access_log");

        Dataset<Row> data = spark.sql("select remote_addr, time_local, request, status, body_bytes_sent, http_referer, http_user_agent from t_access_log");

        data.foreach(e -> {


        });

        data.write().jdbc(url, "t_access_log", props);

        System.out.println(dataFrame.count());

        spark.stop();
    }

}
