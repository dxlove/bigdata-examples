package com.leone.bigdata.spark.java.sql;

import com.leone.bigdata.spark.java.bean.User;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * <p> java 版本 spark sql join
 *
 * @author leone
 * @since 2019-06-25
 **/
public class JavaSparkSqlJoin {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("javaSparkSqlJoin").master("local[*]").getOrCreate();

        // 创建 user dataFrame
        JavaRDD<String> userStringRDD = spark.sparkContext().textFile(args[0], 3).toJavaRDD();

        JavaRDD<User> userJavaRDD = userStringRDD.map((Function<String, User>) s -> {
            String[] fields = s.split(",");
            return new User(Long.valueOf(fields[0]), fields[1], Integer.valueOf(fields[2]), Integer.valueOf(fields[3]), Double.valueOf(fields[4]), fields[5], Boolean.valueOf(fields[6]));
        });

        Dataset<Row> userDataFrame = spark.createDataFrame(userJavaRDD, User.class);


        // 创建 order dataFrame
        JavaRDD<String> orderStringRDD = spark.sparkContext().textFile(args[1], 3).toJavaRDD();

        JavaRDD<Row> orderJavaRDD = orderStringRDD.map((Function<String, Row>) s -> {
            String[] fields = s.split(",");
            return RowFactory.create(Long.valueOf(fields[0]), Long.valueOf(fields[1]), fields[2], Double.valueOf(fields[3]), Double.valueOf(fields[4]), Integer.valueOf(fields[5]), fields[6]);
        });

        List<StructField> schema = new ArrayList<>();
        schema.add(DataTypes.createStructField("orderId", DataTypes.LongType, false));
        schema.add(DataTypes.createStructField("userId", DataTypes.LongType, false));
        schema.add(DataTypes.createStructField("productName", DataTypes.StringType, false));
        schema.add(DataTypes.createStructField("productPrice", DataTypes.DoubleType, false));
        schema.add(DataTypes.createStructField("totalAmount", DataTypes.DoubleType, false));
        schema.add(DataTypes.createStructField("productCount", DataTypes.IntegerType, false));
        schema.add(DataTypes.createStructField("createTime", DataTypes.StringType, false));

        StructType structType = DataTypes.createStructType(schema);
        Dataset<Row> orderDataFrame = spark.createDataFrame(orderJavaRDD, structType);


        userDataFrame.createOrReplaceTempView("t_user");
        userDataFrame.createOrReplaceTempView("t_order");

        spark.sql("select * from t_user join t_order on t_user.userId = t_order.orderId").show();

        spark.stop();
    }

}
