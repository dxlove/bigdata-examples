package com.leone.bigdata.spark.java.sql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;

/**
 * 2.通过动态转换创建dataFrame
 * 自Spark2.0之后，DataFrame和DataSet合并为更高级的DataSet，新的DataSet具有两个不同的API特性
 * 1.非强类型(untyped)，DataSet[Row]是泛型对象的集合，它的别名是DataFrame
 * 2.强类型(strongly-typed)，DataSet[T]是具体对象的集合，如scala和java中定义的类
 *
 * @author leone
 * @since 2019-03-20
 **/
public class JavaDataFrameCreateStructure {

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession.builder().master("local[*]").appName("dataFrame").getOrCreate();

        JavaRDD<String> stringRDD = spark.read().textFile(args[0]).javaRDD();

        // 通过动态转换创建dataFrame
        JavaRDD<Row> rowJavaRDD = stringRDD.map((Function<String, Row>) s -> {
            String[] fields = s.split(",");
            return RowFactory.create(Long.valueOf(fields[0]), fields[1], Integer.valueOf(fields[2]), Integer.valueOf(fields[3]), Double.valueOf(fields[4]), fields[5], Boolean.valueOf(fields[6]));
        });

        ArrayList<StructField> list = new ArrayList<>();
        list.add(DataTypes.createStructField("userId", DataTypes.LongType, true));
        list.add(DataTypes.createStructField("username", DataTypes.StringType, true));
        list.add(DataTypes.createStructField("sex", DataTypes.IntegerType, true));
        list.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));
        list.add(DataTypes.createStructField("credit", DataTypes.DoubleType, true));
        list.add(DataTypes.createStructField("createTime", DataTypes.StringType, true));
        list.add(DataTypes.createStructField("deleted", DataTypes.BooleanType, true));

        StructType structType = DataTypes.createStructType(list);
        Dataset<Row> dataFrame = spark.createDataFrame(rowJavaRDD, structType);

        // 打印表的原数据信息
        dataFrame.printSchema();

        if (args[1] != null) {
            Configuration conf = new Configuration();
            FileSystem fileSystem = FileSystem.get(conf);
            Path path = new Path(args[1]);
            if (fileSystem.exists(path)) {
                fileSystem.delete(path, true);
            }
        }

        dataFrame.select(dataFrame.col("username"), dataFrame.col("age").plus(1), dataFrame.col("createTime"))
                .where("createTime < date_format('2018-01-08', 'yyyy-MM-dd')").write().csv(args[1]);

        // 根据某一列的值进行过滤
        dataFrame.filter(dataFrame.col("age").gt(18)).show();

        // 根据某一列进行分组
        dataFrame.groupBy(dataFrame.col("age")).count().orderBy(dataFrame.col("age")).describe("age").show();

        JavaRDD<Row> javaRDD = dataFrame.toJavaRDD();


        spark.stop();
    }

}
