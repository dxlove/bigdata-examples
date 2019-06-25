package com.leone.bigdata.spark.java.sql;

import com.leone.bigdata.spark.java.bean.User;
import com.leone.bigdata.spark.java.core.JavaRddCheckpoint;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;

/**
 * 自Spark2.0之后，DataFrame和DataSet合并为更高级的DataSet，新的DataSet具有两个不同的API特性
 * 1.非强类型(untyped)，DataSet[Row]是泛型对象的集合，它的别名是DataFrame
 * 2.强类型(strongly-typed)，DataSet[T]是具体对象的集合，如scala和java中定义的类
 *
 * @author leone
 * @since 2019-03-20
 **/
public class JavaDataSetCreate {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("dataSet").getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());

        // 1.通过结构化数据
        Dataset<User> dataset = spark.read().json(args[1]).as(Encoders.bean(User.class));

        // 2.通过spark api 转换为dataSet
        //JavaRDD<String> stringRDD = spark.sparkContext().textFile(args[0], 3).toJavaRDD();

        //RDD<User> userJavaRDD = stringRDD.map((Function<String, User>) s -> {
        //    String[] fields = s.split(",");
        //    return new User(Long.valueOf(fields[0]), fields[1], fields[4], fields[6], Integer.valueOf(fields[5]), fields[2], Boolean.valueOf(fields[4]));
        //}).rdd();

        //Dataset<User> dataset = spark.createDataset(userJavaRDD, Encoders.bean(User.class));

        // 等于sql中查询所有
        dataset.show();

        // 打印表的原数据信息
        dataset.printSchema();

        dataset.select(dataset.col("username"), dataset.col("age").plus(1)).show();

        // 根据某一列的值进行过滤
        dataset.filter(dataset.col("age").gt(18)).show();

        // 根据某一列进行分组
        dataset.groupBy(dataset.col("age")).count().orderBy(dataset.col("age")).describe("age").show();

        spark.stop();
    }

}
