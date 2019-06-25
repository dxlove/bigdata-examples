package com.leone.bigdata.spark.java.sql;

import com.leone.bigdata.spark.java.bean.User;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Date;

/**
 * <p> 自Spark2.0之后，DataFrame和DataSet合并为更高级的DataSet，新的DataSet具有两个不同的API特性
 * 1.非强类型(untyped)，DataSet[Row]是泛型对象的集合，它的别名是DataFrame
 * 2.强类型(strongly-typed)，DataSet[T]是具体对象的集合，如scala和java中定义的类
 *
 * @author leone
 * @since 2019-03-20
 **/
public class JavaDataFrameCreateReflect {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("dataFrame").getOrCreate();
        // 1.直接通过json数据格式创建DataFrame spark2.x中 dataFrame 等于是 dataset + 泛型 Row
        //Dataset<Row> dataFrame = spark.read().json("file:///root/logs/json/");

        JavaRDD<String> stringRDD = spark.read().textFile(args[0]).javaRDD();

        // 2.通过RDD创建，通过Java反射转换
        JavaRDD<User> userJavaRDD = stringRDD.map((Function<String, User>) s -> {
            String[] fields = s.split(",");
            return new User(Long.valueOf(fields[0]), fields[1], Integer.valueOf(fields[2]), Integer.valueOf(fields[3]), Double.valueOf(fields[4]), fields[5], Boolean.valueOf(fields[6]));
        });
        Dataset<Row> dataFrame = spark.sqlContext().createDataFrame(userJavaRDD, User.class);

        // 等于sql中查询所有
        dataFrame.show();

        // 打印表的原数据信息
        dataFrame.printSchema();

        dataFrame.select(dataFrame.col("username"), dataFrame.col("age").plus(1), dataFrame.col("createTime"))
                .where("createTime < date_format('2018-01-08', 'yyyy-MM-dd')").write().csv(args[1]);

        // 根据某一列的值进行过滤
        dataFrame.filter(dataFrame.col("age").gt(18)).show();

        // 根据某一列进行分组
        dataFrame.groupBy(dataFrame.col("age")).count().orderBy(dataFrame.col("age")).describe("age").show();

        spark.stop();
    }

}
