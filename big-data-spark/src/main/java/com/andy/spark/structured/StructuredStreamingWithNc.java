package com.andy.spark.structured;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-29
 **/
public class StructuredStreamingWithNc {

    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession.builder().appName("structured").master("local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("warn");

        Dataset<Row> line = spark.readStream()
                .format("socket")
                .option("host", "ip")
                .option("port", "8000")
                .load();

        Dataset<String> words = line.as(Encoders.STRING())
                .flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator(), Encoders.STRING());

        Dataset<Row> wordCount = words.groupBy("value").count();

        StreamingQuery query = wordCount.writeStream()
                .outputMode(OutputMode.Complete())
                .format("console")
                .start();

        query.awaitTermination();
    }

}
