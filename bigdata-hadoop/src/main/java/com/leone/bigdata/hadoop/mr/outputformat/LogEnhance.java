package com.leone.bigdata.hadoop.mr.outputformat;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * <p>
 *
 * @author leone
 * @since 2019-03-22
 **/
public class LogEnhance {

    static class LogEnhanceMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Map<String, String> ruleMap = new HashMap<>();

        Text k = new Text();
        NullWritable v = NullWritable.get();

        /**
         * 初始化时从数据库中加载规则信息倒ruleMap中
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                DBLoader.dbLoader(ruleMap);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 获取一个计数器用来记录不合法的日志行数, 组名, 计数器名称
            Counter counter = context.getCounter("malformed", "malformedline");
            String line = value.toString();
            String[] fields = StringUtils.split(line, "\t");
            try {
                String url = fields[26];
                String content_tag = ruleMap.get(url);
                // 判断内容标签是否为空，如果为空，则只输出url到待爬清单；如果有值，则输出到增强日志
                if (content_tag == null) {
                    k.set(url + "\t" + "tocrawl" + "\n");
                    context.write(k, v);
                } else {
                    k.set(line + "\t" + content_tag + "\n");
                    context.write(k, v);
                }

            } catch (Exception exception) {
                counter.increment(1);
            }
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(LogEnhance.class);

        job.setMapperClass(LogEnhanceMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(LogEnhancerOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = outputPath.getFileSystem(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        // 不需要reducer
        job.setNumReduceTasks(0);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
