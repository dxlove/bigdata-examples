package com.leone.hadoop.mr.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * <p>
 *
 * @author leone
 * @since 2018-11-21
 **/
public class WcMain {

    public static void main(String[] args) throws Exception {
        // 默认以本地模式运行
        Configuration conf = new Configuration();
        // 设置job运行时要访问的默认文件系统
        conf.set("fs.defaultFS", "hdfs://node-1:9000");
        // 设置job提交到哪去运行
        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.resourcemanager.hostname", "node-1");

        Job job = Job.getInstance();

        // 设置 job 的信息
        job.setJarByClass(WcMain.class);
        // job 的 jar 包路径
        job.setJar(args[2]);
        // job 名称
        job.setJobName("WordCount");


        // 设置 job 的 combiner
        job.setCombinerClass(WcCombiner.class);

        // 设置 job 的inputFormat 如果不设置默认是 TextInputFormat
        // job.setInputFormatClass(CombineFileInputFormat.class);
        // CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        // CombineTextInputFormat.setMinInputSplitSize(job, 2097152);

        // 封装参数:本次job所要调用的Mapper实现类、Reducer实现类
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        // 封装参数：本次job的Mapper实现类、Reducer实现类产生的结果数据的key、value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置输出结果key value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置 job 输出结果路径，如果存在就删除
        Path output = new Path(args[1]);

        FileSystem fileSystem = output.getFileSystem(conf);
        if (fileSystem.exists(output)) {
            fileSystem.delete(output, true);
        }

        // 设置 job 输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);

        // 封装参数：想要启动的reduce task的数量
        job.setNumReduceTasks(2);

        // 执行作业
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
