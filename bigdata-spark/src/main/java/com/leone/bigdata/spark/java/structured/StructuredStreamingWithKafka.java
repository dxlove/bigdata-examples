package com.leone.bigdata.spark.java.structured;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

/**
 * <p>
 *
 * @author leone
 * @since 2019-04-02
 **/
public class StructuredStreamingWithKafka {

    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("structured").getOrCreate();
        spark.sparkContext().setLogLevel("warn");

        // Create DataSet representing the stream of input lines from kafka
        Dataset<String> line = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "node-2:9092,node-3:9092,node-4:9092")
                .option("subscribe", "structured-topic")
                .load()
                .selectExpr("CAST(value AS STRING)")
                .as(Encoders.STRING());

        // val kafkaDS: Dataset[(String, String)] = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

        line.printSchema();

        Dataset<Row> wordCounts = line.flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(),
                Encoders.STRING()).groupBy("value").count();

        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();

    }

}
