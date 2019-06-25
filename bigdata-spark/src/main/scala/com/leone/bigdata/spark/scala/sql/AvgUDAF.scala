package com.leone.bigdata.spark.scala.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * <p>
  *
  * @author leone
  * @since 2019-06-25
  **/
object AvgUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    // 给定UDAF的输出参数类型
    StructType(
      StructField("sal", DoubleType) :: Nil
    )
  }

  override def bufferSchema: StructType = {
    // 在计算过程中会涉及到的缓存数据类型
    StructType(
      StructField("total_sal", DoubleType) ::
        StructField("count_sal", LongType) :: Nil
    )
  }

  override def dataType: DataType = {
    // 给定该UDAF返回的数据类型
    DoubleType
  }

  override def deterministic: Boolean = {
    // 主要用于是否支持近似查找，如果为false：表示支持多次查询允许结果不一样，为true表示结果必须一样
    true
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    // 初始化 ===> 初始化缓存数据
    buffer.update(0, 0.0) // 初始化total_sal
    buffer.update(1, 0L) // 初始化count_sal
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    // 根据输入的数据input，更新缓存buffer的内容
    // 获取输入的sal数据
    val inputSal = input.getDouble(0)

    // 获取缓存中的数据
    val totalSal = buffer.getDouble(0)
    val countSal = buffer.getLong(1)

    // 更新缓存数据
    buffer.update(0, totalSal + inputSal)
    buffer.update(1, countSal + 1L)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    // 当两个分区的数据需要进行合并的时候，该方法会被调用
    // 功能：将buffer2中的数据合并到buffer1中
    // 获取缓存区数据
    val buf1Total = buffer1.getDouble(0)
    val buf1Count = buffer1.getLong(1)

    val buf2Total = buffer2.getDouble(0)
    val buf2Count = buffer2.getLong(1)

    // 更新缓存区
    buffer1.update(0, buf1Total + buf2Total)
    buffer1.update(1, buf1Count + buf2Count)
  }

  override def evaluate(buffer: Row): Any = {
    // 求返回值
    buffer.getDouble(0) / buffer.getLong(1)
  }
}
