package com.leone.spark.core.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * <p> Action （行动）算子 14 个
 *
 * @author leone
 * @since 2018-12-19
 **/
public class JavaActionOperation {

    private static List<Integer> numberList = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

    /**
     * reduce 算子，通过func函数聚集RDD中的所有元素，这个功能必须是可交换且可并联的
     */
    @Test
    public void reduce() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("reduce").setMaster("local[*]"));
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(numberList);
        Integer reduce = javaRDD.reduce((Function2<Integer, Integer, Integer>) Integer::sum);
        System.out.println(reduce);
        sparkContext.close();
    }


    /**
     * collect 算子，在驱动程序中，以数组的形式返回数据集的所有元素
     */
    @Test
    public void collect() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("collect").setMaster("local[*]"));
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(numberList);
        JavaRDD<Integer> map = javaRDD.map((Function<Integer, Integer>) s -> s * 2);
        List<Integer> collect = map.collect();
        for (Integer num : collect) {
            System.out.println(num);
        }
        sparkContext.close();
    }

    /**
     * count 算子，返回RDD的元素个数
     */
    @Test
    public void count() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("count").setMaster("local[*]"));
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(numberList);
        long count = javaRDD.count();
        System.out.println(count);
        sparkContext.close();
    }

    /**
     * first 算子，返回RDD的第一个元素,类似于take(1)
     */
    @Test
    public void first() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("first").setMaster("local[*]"));
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(numberList);
        Integer first = javaRDD.first();
        System.out.println(first);
        sparkContext.close();
    }


    /**
     * take 算子，返回一个由数据集的前n个元素组成的数组
     */
    @Test
    public void take() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("take").setMaster("local[*]"));
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(numberList);
        List<Integer> take = javaRDD.take(5);
        System.out.println(Arrays.toString(take.toArray()));
        sparkContext.close();
    }

    /**
     * takeSample 算子,返回一个数组，该数组由从数据集中随机采样的num个元素组成，可以选择是否用随机数替换不足的部分，seed用于指定随机数生成器种子
     * boolean: 该参数对应true和false，分别对应两种算法（两种抽取器）这两种算法分别为：PoissonSampler 和 BernoulliSampler
     * num： 为要随机抽取多少个在Rdd中的元素
     * seed : 即种子，在算法中充当着随机参数，根据随机参数的不同，最后产生的结果不同，seed参数相同，使用的算法boolean也相同，
     */
    @Test
    public void takeSample() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("action-takeSample").setMaster("local[*]"));
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(numberList);

        /*
         * false 不可以多次抽样
         * 样本个数num大于父本个数时，只能返回父本个数 15 -> result:[3, 8, 7, 9, 2, 1, 6, 5, 4]
         * 样本个数num小于父本个数时，返回样本个数     5 -> result:[8, 9, 3, 1, 2]
         */

        /*
         * true 可以多次抽样
         * 样本个数num大于父本个数时，样本个数num大于父本个数时，返回样本个数 15 -> result:[6, 1, 6, 7, 6, 7, 5, 1, 5, 8, 6, 7, 5, 7, 3]
         * 样本个数num小于父本个数时，样本个数num小于父本个数时，返回样本个数  5 -> result:[6, 5, 2, 8, 6]
         */

        List<Integer> integers = javaRDD.takeSample(true, 15);
        System.out.println(Arrays.toString(integers.toArray()));
        sparkContext.close();
    }

    /**
     * sample 算子
     */
    @Test
    public void sample() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("action-sample").setMaster("local[*]"));
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(numberList);

        /*
         * false 不可以多次抽样
         * 每个元素被抽取到的概率为0.5：fraction=0.5
         * (false, 0.5) -> result: 3 4 5 2 7
         */

        /*
         * true 可以多次抽样
         * 每个元素被抽取到的期望次数为2：fraction=2
         * (true, 2) -> result: 1 1 1 2 2 2 8 8 9 9 5 5 6 6 6 6 6 3 3 4
         */
        JavaRDD<Integer> result = javaRDD.sample(true, 2);
        result.foreach(x -> System.out.println(x + ""));
        sparkContext.close();
    }


    /**
     * foreach 算子,foreach操作是直接调迭代rdd中每一条数据进行function操作。
     */
    @Test
    public void foreach() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("sample").setMaster("local[*]"));
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(numberList);
        javaRDD.foreach(e -> System.out.println(e + ""));
        sparkContext.close();
    }


    /**
     * saveAsTextFile 算子
     */
    @Test
    public void saveAsTextFile() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("saveAsTextFile").setMaster("local[*]"));
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(numberList);
        // numbers.saveAsTextFile("htfs://node-1:9000/tmp.txt");
        javaRDD.saveAsTextFile("file:///e:/tmp/saveAsTextFile");
        sparkContext.close();
    }

    /**
     * saveAsObjectFile 算子
     */
    @Test
    public void saveAsObjectFile() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("saveAsObjectFile").setMaster("local[*]"));
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(numberList);
        javaRDD.saveAsObjectFile("file:///e:/tmp/saveAsObjectFile");
        sparkContext.close();
    }

    /**
     * takeOrdered 算子,从指定的RDD中返回前k个(最小)元素的集合，底层排序，和top相反。
     */
    @Test
    public void takeOrdered() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("takeOrdered").setMaster("local[*]"));
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(numberList);
        List<Integer> result = javaRDD.takeOrdered(3);
        System.out.println(Arrays.toString(result.toArray()));
        sparkContext.close();
    }

    /**
     * top 算子,底层调用的是takeOrdered，之后排序翻转
     */
    @Test
    public void top() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("top").setMaster("local[*]"));
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(numberList);
        List<Integer> result = javaRDD.top(5);
        System.out.println(Arrays.toString(result.toArray()));
        sparkContext.close();
    }

    /**
     * countByKey 算子
     */
    @Test
    public void countByKey() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("countByKey").setMaster("local[*]"));
        List<Tuple2<String, String>> scoresList = Arrays.asList(
                new Tuple2<>("class1", "20"),
                new Tuple2<>("class2", "68"),
                new Tuple2<>("class2", "85"),
                new Tuple2<>("class3", "97"),
                new Tuple2<>("class1", "82")
        );
        JavaPairRDD<String, String> students = sparkContext.parallelizePairs(scoresList);

        Map<String, Long> count = students.countByKey();
        for (Map.Entry<String, Long> stu : count.entrySet()) {
            System.out.println("stu: " + stu.getKey() + " --- val: " + stu.getValue());
        }
        sparkContext.close();
    }


    /**
     * fold 算子
     * 通过op函数聚合各分区中的元素及合并各分区的元素，op函数需要两个参数，在开始时第一个传入的参数为zeroValue,T为RDD数据集的数据类型，，其作用相当于SeqOp和comOp函数都相同的aggregate函数
     */
    @Test
    public void fold() {
        JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf().setAppName("fold").setMaster("local[*]"));
        JavaRDD<Integer> javaRDD = sparkContext.parallelize(numberList);
        Integer result = javaRDD.fold(0, (Function2<Integer, Integer, Integer>) Integer::sum);
        System.out.println(result);
        sparkContext.close();
    }


}
