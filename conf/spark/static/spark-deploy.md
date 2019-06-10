# spark

## 解决 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable

`export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native/:$LD_LIBRARY_PATH`

## 解决yarn提交任务缓慢

```$xslt
19/06/09 15:26:23 WARN yarn.Client: Neither spark.yarn.jars nor spark.yarn.archive is set, falling back to uploading libraries under SPARK_HOME.
```
将 $SPARK_HOME/jars/* 下spark运行依赖的jar上传到hdfs上。

hdfs dfs -mkdir -p hdfs://node-1:9000/spark/jars
hdfs dfs -put  $SPARK_HOME/jars/* hdfs://node-1:9000/spark/jars

## 解决 MAC WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
export JAVA_LIBRARY_PATH="/usr/local/Cellar/hadoop/2.9.2/lib/native/"

