package com.leone.bigdata.spark.java.sql;

import com.leone.bigdata.spark.java.bean.Student;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-20
 **/
public class Rdd2DataFrameReflection {

    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("rdd").setMaster("local[*]"));
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> javaRDD = sc.textFile(args[0]);

        JavaRDD<Student> studentJavaRDD = javaRDD.map((Function<String, Student>) s -> {
            String[] lines = s.split(",");
            return new Student(Integer.parseInt(lines[0]), lines[1], Integer.parseInt(lines[2]), Integer.parseInt(lines[3]), Integer.parseInt(lines[4]), Integer.parseInt(lines[5]));
        });

        // 通过反射的方式将javaRDD转换为dataFrame
        Dataset<Row> dataFrame = sqlContext.createDataFrame(studentJavaRDD, Student.class);

        // 将 dataFrame 注册成为一个临时表
        dataFrame.registerTempTable("t_student");

        dataFrame.show();

        // 使用sql语句进行查询
        Dataset<Row> rowDataset = sqlContext.sql("select id,name,age from t_student where age < 18");
        rowDataset.show();

        // 将dataFrame转换为java的RDD
        JavaRDD<Row> rowJavaRDD = rowDataset.javaRDD();

        // 将rdd中的数据进行映射成Student
        JavaRDD<Student> map = rowJavaRDD.map((Function<Row, Student>) row -> new Student(row.getInt(0), row.getString(1), row.getInt(2), row.getInt(3), row.getInt(4), row.getInt(5)));

        List<Student> collect = map.collect();

        System.out.println(collect);

        sc.stop();
    }


}
