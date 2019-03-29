package com.andy.spark.core.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * <p>
  *
  * @author leone
  * @since 2018-12-18
  **/
object ScalaTransformationOperation {

  val numbers = Array(1, 2, 3, 4, 5, 7, 9, 9)


  /**
    * map 算子:将原来 RDD 的每个数据项通过 map 中的用户自定义函数 f 映射转变为一个新的元素。源码中 map 算子相当于初始化一个 RDD， 新 RDD 叫做 MappedRDD(this, sc.clean(f))。
    */
  def map(): Unit = {
    val conf = new SparkConf().setAppName("map").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 在Spark中创建RDD的创建方式大概可以分为三种：1.从集合中创建RDD；2.从外部存储创建RDD；3.从其他RDD创建。
    val rdd = sc.parallelize(numbers, 1)

    // val multipleNumberRDD = rdd.map { num => num * 2 }
    // val multipleNumberRDD = rdd.map(_ * 2)
    val multipleNumberRDD = rdd.map(i => i * 2)

    multipleNumberRDD.foreach { num => println(num) }

    sc.stop()
  }

  /**
    * flatMap 算子: 将原来 RDD 中的每个元素通过函数 f 转换为新的元素，并将生成的 RDD 的每个集合中的元素合并为一个集合，内部创建 FlatMappedRDD(this，sc.clean(f))。
    */
  def flatMap(): Unit = {
    val conf = new SparkConf().setAppName("flatMap").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines = Array("hello you", "hello me", "hello world")

    val rdd = sc.parallelize(lines)

    // val words: RDD[String] = linesRDD.flatMap {
    //   _.split(" ")
    // }

    // rdd.flatMap(i => i.split(" "))
    rdd.flatMap(_.split(" "))
    rdd.foreach(println)

    sc.stop()
  }


  /**
    * filter算子: filter 函数功能是对元素进行过滤，对每个元应用f函数，返回值为 true 的元素 在RDD 中保留，返回值为 false 的元素将被过滤掉。 内部实现相当于生成 FilteredRDD(this，sc.clean(f))。
    */
  def filter(): Unit = {
    val conf = new SparkConf().setAppName("filter").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(numbers)

    // 一下三种写法都成立
    // val result = rdd.filter(num => num % 2 == 0)
    // val result = rdd.filter(_ % 2 == 0)
    val result = rdd.filter { i => i % 2 == 0 }

    result.foreach(println)
    sc.stop()
  }


  /**
    * groupByKey 算子: 在一个(K,V)的RDD上调用，返回一个(K, Iterator[V])的RDD
    */
  def groupByKey(): Unit = {
    val conf = new SparkConf().setAppName("groupByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val score = Array(Tuple2("class1", 80), Tuple2("class2", 70), Tuple2("class3", 60), Tuple2("class2", 79), Tuple2("class1", 85), Tuple2("class2", 77))

    val rdd = sc.parallelize(score, 1)

    val result = rdd.groupByKey()

    result.foreach(println)
    sc.stop()
  }

  /**
    * reduceByKey 算子: 在一个(K,V)的RDD上调用，返回一个(K,V)的RDD，使用指定的reduce函数，将相同key的值聚合到一起，与groupByKey类似，reduce任务的个数可以通过第二个可选的参数来设置
    */
  def reduceByKey(): Unit = {
    val conf = new SparkConf().setAppName("reduceByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val score = Array(Tuple2("class1", 80), Tuple2("class2", 70), Tuple2("class3", 60), Tuple2("class2", 79), Tuple2("class1", 85), Tuple2("class2", 77))

    val rdd = sc.parallelize(score, 2)

    val result = rdd.reduceByKey(_ + _)

    result.foreach(println)
    sc.stop()
  }

  /**
    * sortByKey 算子
    * 在一个(K,V)的RDD上调用，K必须实现Ordered接口，返回一个按照key进行排序的(K,V)的RDD
    */
  def sortByKey(): Unit = {
    val conf = new SparkConf().setAppName("sortByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val array = Array(Tuple2(79, "jack"), Tuple2(86, "tom"), Tuple2(95, "andy"), Tuple2(90, "james"))
    val rdd = sc.parallelize(array, 2)

    val result = rdd.sortByKey(ascending = false)

    result.foreach(println)
    sc.stop()
  }

  /**
    * sortBy 算子
    * 与sortByKey类似，但是更灵活,可以用func先对数据进行处理，按照处理后的数据比较结果排序。
    */
  def sortBy(): Unit = {
    val conf = new SparkConf().setAppName("sortBy").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(numbers, 2)

    val result1 = rdd.sortBy(x => x).collect()

    val result2 = rdd.sortBy(x => x % 3).collect()

    println(result1.toBuffer)

    println(result2.toBuffer)

    sc.stop()
  }

  /**
    * join 算子(otherDataset, [numTasks])是连接操作，将输入数据集(K,V)和另外一个数据集(K,W)进行Join， 得到(K, (V,W))；该操作是对于相同K的V和W集合进行笛卡尔积 操作，也即V和W的所有组合；
    */
  def join(): Unit = {
    val conf = new SparkConf().setAppName("join").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val studentList = Array(new Tuple2[Integer, String](1001, "tom"), new Tuple2[Integer, String](1002, "jack"), new Tuple2[Integer, String](1003, "james"), new Tuple2[Integer, String](1004, "andy"))

    val scoreList = Array(new Tuple2[Integer, Integer](1001, 100), new Tuple2[Integer, Integer](1002, 90), new Tuple2[Integer, Integer](1003, 89), new Tuple2[Integer, Integer](1004, 97))

    val pairRdd1 = sc.parallelize(studentList)
    val pairRdd2 = sc.parallelize(scoreList)

    val result = pairRdd1.join(pairRdd2)

    result.foreach(println)
    sc.stop()
  }

  /**
    * combineByKey 算子
    */
  def combineByKey(): Unit = {
    val conf = new SparkConf().setAppName("combineByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Array(1, 2, 3, 4, 6, 7, 8))
    val rdd1 = rdd.map((_, 1))
    val rdd2 = rdd1.combineByKey(x => x, (a: Int, b: Int) => a + b, (m: Int, n: Int) => m + n)
    rdd2.foreach(e => println(e))

    sc.stop()
  }


  /**
    * mapPartitions 算子
    */
  def mapPartitions(): Unit = {
    val conf = new SparkConf().setAppName("mapPartitions").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(numbers, 3)

    val result = rdd.mapPartitions(iter => {
      println("james")
      var res = List[(Int, Int)]()
      while (iter.hasNext) {
        val cur = iter.next()
        res.::=(cur, cur * 2)
      }
      res.iterator
    })

    result.foreach(e => {
      println("-----")
      println(e)
    })

    sc.stop()
  }


  /**
    * glom 算子 将每个分区形成一个数组
    */
  def glom(): Unit = {
    val conf = new SparkConf().setAppName("glom").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(numbers, 2)

    val result = rdd.glom().collect()

    result.foreach(e => println(e.toBuffer))

    sc.stop()
  }


  /**
    * union 算子 对源RDD和参数RDD求并集后返回一个新的RDD
    */
  def union(): Unit = {
    val conf = new SparkConf().setAppName("union").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(numbers, 2)
    val rdd2 = sc.parallelize(1 to 10, 2)

    val result = rdd1.union(rdd2)

    result.foreach(e => {
      println("--")
      println(e)
    })

    sc.stop()
  }


  /**
    * intersection 算子 对源RDD和参数RDD求交集后返回一个新的RDD
    */
  def intersection(): Unit = {
    val conf = new SparkConf().setAppName("intersection").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(numbers, 2)
    val rdd2 = sc.parallelize(5 to 8, 2)


    val result = rdd1.intersection(rdd2)

    result.foreach(println)

    sc.stop()
  }


  /**
    * leftOuterJoin 算子 根据两个RDD来进行做外连接，右边没有的值会返回一个None。右边有值的话会返回一个Some。
    */
  def leftOuterJoin(): Unit = {
    val conf = new SparkConf().setAppName("leftOuterJoin").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(numbers, 2).map((_, 1))
    val rdd2 = sc.parallelize(1 to 10, 2).map((_, 2))

    val result = rdd1.leftOuterJoin(rdd2)

    result.foreach(println)

    sc.stop()
  }

  /**
    * rightOuterJoin 算子 对两个RDD来做一个右外链接。返回的Value类型为option类型。左边有值的话为Some，没有的话为None。
    */
  def rightOuterJoin(): Unit = {
    val conf = new SparkConf().setAppName("rightOuterJoin").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(numbers, 2).map((_, 1))
    val rdd2 = sc.parallelize(1 to 10, 2).map((_, 2))

    val result = rdd1.rightOuterJoin(rdd2)

    result.foreach(println)

    sc.stop()
  }

  /**
    * cartesian 算子 对两个RDD内的所有元素进行笛卡尔积操作。操作后，内部实现返回CartesianRDD；
    */
  def cartesian(): Unit = {
    val conf = new SparkConf().setAppName("cartesian").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(numbers, 2)
    val rdd2 = sc.parallelize(1 to 10, 2)

    val result = rdd1.cartesian(rdd2)

    result.foreach(println)

    sc.stop()
  }

  /**
    * coalesce 算子 用于将RDD进行重分区，使用HashPartitioner。且该RDD的分区个数等于numPartitions个数。如果shuffle设置为true，则会进行shuffle
    */
  def coalesce(): Unit = {
    val conf = new SparkConf().setAppName("coalesce").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(numbers, 6)
    println(rdd1.getNumPartitions)

    val result = rdd1.coalesce(3)
    println(result.getNumPartitions)

    sc.stop()
  }

  /**
    * repartition 算子 repartition是coalesce接口中shuffle为true的简易实现，即Reshuffle RDD并随机分区，使各分区数据量尽可能平衡。若分区之后分区数远大于原分区数，则需要shuffle。
    */
  def repartition(): Unit = {
    val conf = new SparkConf().setAppName("repartition").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(numbers, 2)
    println(rdd1.getNumPartitions)
    println(rdd1.collect().toBuffer)

    val rdd2 = rdd1.repartition(5)

    println(rdd2.getNumPartitions)
    println(rdd2.collect().toBuffer)

    sc.stop()
  }


  /**
    * mapPartitionsWithIndex 算子 类似于mapPartitions，但func带有一个整数参数表示分片的索引值，因此在类型为T的RDD上运行时，func的函数类型必须是(Int, Iterator[T]) => Iterator[U])
    */
  def mapPartitionsWithIndex(): Unit = {
    val conf = new SparkConf().setAppName("mapPartitionsWithIndex").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(numbers, 2)

    sc.stop()
  }

  /**
    * cogroup 算子:对两个RDD中的KV元素，每个RDD中相同key中的元素分别聚合成一个集合。与reduceByKey不同的是针对两个RDD中相同的key的元素进行合并。
    */
  def cogroup(): Unit = {
    val conf = new SparkConf().setAppName("cogroup").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val nameArray = Array(Tuple2(1, "Spark"), Tuple2(2, "Hadoop"), Tuple2(3, "Flume"), Tuple2(4, "Hive"))

    val typeArray = Array(Tuple2(1, "james"), Tuple2(2, "andy"), Tuple2(3, "jack"), Tuple2(4, "jerry"), Tuple2(3, "tom"),
      Tuple2(1, "34"), Tuple2(1, "45"), Tuple2(2, "47"), Tuple2(3, "75"), Tuple2(4, "95"), Tuple2(3, "16"), Tuple2(1, "85"))

    val names = sc.parallelize(nameArray)

    val types = sc.parallelize(typeArray)

    val nameAndType = names.cogroup(types)

    nameAndType.foreach(println)

    sc.stop()
  }

  /**
    * sample 算子 根据fraction指定的比例对数据进行采样，可以选择是否使用随机数进行替换，seed用于指定随机数生成器种子
    */
  def sample(): Unit = {
    val conf = new SparkConf().setAppName("sample").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(numbers, 2)

    val result = rdd.sample(true, 0.5)

    result.foreach(println)
    sc.stop()
  }

  /**
    * distinct 算子 对源RDD进行去重后返回一个新的RDD
    */
  def distinct(): Unit = {
    val conf = new SparkConf().setAppName("distinct").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(1, 2, 3, 2, 1, 5), 2)
    val result = rdd1.distinct()
    result.foreach(println)
    sc.stop()
  }

  /**
    * aggregateByKey 算子
    * 在kv对的RDD中，，按key将value进行分组合并，合并时，将每个value和初始值作为seq函数的参数，进行计算，返回的结果作为一个新的kv对，然后再将结果按照key进行合并，最后将每个分组的value传递给combine函数进行计算（先将前两个value进行计算，将返回结果和下一个value传给combine函数，以此类推），将key与计算结果作为一个新的kv对输出。
    * seqOp函数用于在每一个分区中用初始值逐步迭代value，combOp函数用于合并每个分区中的结果
    */
  def aggregateByKey(): Unit = {
    val conf = new SparkConf().setAppName("aggregateByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)

    val rdd2 = rdd1.aggregateByKey(0)(math.max, _ + _)
    val result = rdd2.collect()
    println(result.toBuffer)

    sc.stop()
  }

  /**
    * pipe 算子 通过一个shell命令来对RDD各分区进行“管道化”。通过pipe变换将一些shell命令用于Spark中生成的新RDD
    */
  def pipe(): Unit = {
    val conf = new SparkConf().setAppName("aggregateByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(numbers, 2)

    val scriptPath = "/root/echo.bat"

    val pipeRDD = rdd1.pipe(scriptPath)

    println(pipeRDD.collect().toBuffer)

    sc.stop()
  }

  /**
    * partitionBy算子
    * 对RDD进行分区操作，如果原有的partitionRDD和现有的partitionRDD是一致的话就不进行分区，
    * 否则会生成ShuffleRDD.
    */
  def partitionBy(): Unit = {
    val conf = new SparkConf().setAppName("partitionBy").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(numbers, 3)

    println(rdd1.getNumPartitions)
    sc.stop()
  }

  /**
    * foldByKey算子
    * aggregateByKey的简化操作，seqop和combop相同，
    */
  def foldByKey(): Unit = {
    val conf = new SparkConf().setAppName("foldByKey").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List((1, 3), (1, 2), (1, 4), (2, 3), (3, 6), (3, 8)), 3)

    val rdd2 = rdd1.foldByKey(0)(_ + _)

    println(rdd2.getNumPartitions)
    println(rdd2.collect().toBuffer)

    sc.stop()
  }


  /**
    * 缓存算子
    */
  def cache(): Unit = {
    val conf = new SparkConf().setAppName("cache").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(numbers)
    // 将此rdd的缓存到内存中 只有遇到action算子才会缓存
    val rdd2 = rdd1.cache()
    rdd2.count()
    // 当执行此方法，持久化的数据就会被删除:
    rdd1.unpersist(true)

    sc.stop()
  }


  /**
    * glomc()算子 将每一个分区形成一个数组，形成新的RDD类型时RDD[Array[T]]
    */
  def glomc(): Unit = {
    val conf = new SparkConf().setAppName("glomc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(numbers)
    // 将此rdd的缓存到内存中 只有遇到action算子才会缓存
    val rdd2 = rdd1.glom()

    rdd2.collect().foreach(e => {
      println(e.toBuffer)
    })

    sc.stop()
  }

  /**
    * subtract()算子 计算差的一种函数去除两个RDD中相同的元素，不同的RDD将保留下来
    */
  def subtract(): Unit = {
    val conf = new SparkConf().setAppName("subtract").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(numbers)
    val rdd2 = sc.parallelize(5 to 10)

    val rdd3 = rdd1.subtract(rdd2)

    rdd3.collect().foreach(println)

    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    subtract()
  }


}
