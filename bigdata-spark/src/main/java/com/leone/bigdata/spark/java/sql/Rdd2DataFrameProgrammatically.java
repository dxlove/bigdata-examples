package com.leone.bigdata.spark.java.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.LinkedList;
import java.util.List;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-20
 **/
public class Rdd2DataFrameProgrammatically {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession.builder().master("local[5]").appName("rdd").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> javaRDD = jsc.textFile(args[0]);

        // 创建javaRDD
        JavaRDD<Row> rows = javaRDD.map((Function<String, Row>) s -> {
            String[] lines = s.split(",");
            return RowFactory.create(Integer.valueOf(lines[0]), lines[1], Integer.valueOf(lines[2]));
        });

        // 动态创建原数据
        List<StructField> fieldList = new LinkedList<>();
        fieldList.add(DataTypes.createStructField("id", DataTypes.IntegerType, true));
        fieldList.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        fieldList.add(DataTypes.createStructField("age", DataTypes.IntegerType, true));

        StructType structType = DataTypes.createStructType(fieldList);
        Dataset<Row> dataFrame = spark.createDataFrame(rows, structType);

        dataFrame.registerTempTable("t_student");

        Dataset<Row> sql = spark.sql("select id,name,age from t_student where age < 25 order by age desc");

        sql.show();

        List<Row> rowList = dataFrame.javaRDD().collect();
        for (Row row : rowList) {
            System.out.println(row);
        }

    }

}
