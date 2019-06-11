package com.leone.bigdata.hadoop.mr.applog;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <p> 数据清洗
 *
 * @author leone
 * @since 2019-03-27
 **/
public class AppLogClean {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public static class AppLogCleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        Text k = null;
        NullWritable v = null;
        SimpleDateFormat sdf = null;
        // 多路输出器
        MultipleOutputs<Text, NullWritable> mos = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            k = new Text();
            v = NullWritable.get();
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            mos = new MultipleOutputs<>(context);
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            AppLogBean logBean = objectMapper.readValue(value.toString(), new TypeReference<AppLogBean>() {
            });

            AppLogBean.Header header = logBean.getHeader();
            if (!ObjectUtils.allNotNull(header)) {
                return;
            }

            /*
             * 过滤缺失必选字段的记录
             */
            if (StringUtils.isEmpty(header.getSdk_ver()) || StringUtils.isEmpty(header.getApp_id())) {
                return;
            }

            if (StringUtils.isEmpty(header.getApp_token()) || StringUtils.isEmpty(header.getApp_ver_code())) {
                return;
            }

            if (StringUtils.isEmpty(header.getCommit_time()) || StringUtils.isEmpty(header.getCommit_id()) || StringUtils.isEmpty(header.getCid_sn())) {
                return;
            } else {
                // 练习时追加的逻辑，替换掉原始数据中的时间戳
                String commit_time = header.getCommit_time();
                String format = sdf.format(new Date(Long.parseLong(commit_time) + 38 * 24 * 60 * 60 * 1000L));
                header.setCommit_time(format);
            }

            if (StringUtils.isEmpty(header.getPid()) || (StringUtils.isEmpty(header.getDevice_id()) && header.getDevice_id().length() < 17)) {
                return;
            }

            if (StringUtils.isEmpty(header.getOs_ver()) || StringUtils.isEmpty(header.getOs_name()) || StringUtils.isEmpty(header.getLanguage())) {
                return;
            }

            if (StringUtils.isEmpty(header.getCountry()) || StringUtils.isEmpty(header.getManufacture()) || StringUtils.isEmpty(header.getDevice_model())) {
                return;
            }

            if (StringUtils.isEmpty(header.getResolution()) || StringUtils.isEmpty(header.getNet_type())) {
                return;
            }

            String user_id;
            if ("android".equals(header.getOs_name())) {
                user_id = StringUtils.isEmpty(header.getAndroid_id()) ? header.getAndroid_id() : header.getDevice_id();
            } else {
                user_id = header.getDevice_id();
            }
            // 输出结果
            header.setUser_id(user_id);
            if (header.getUser_id().equals("m.e4:c7:63:a7:d3:30")) {
                System.err.println(header.toString());
            }
            k.set(header.toString());
            if ("android".equals(header.getOs_name())) {
                mos.write(k, v, "android/android");
            } else if ("ios".equals(header.getOs_name())) {
                mos.write(k, v, "ios/ios");
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJobName("app-log");
        job.setJarByClass(AppLogClean.class);
        job.setMapperClass(AppLogCleanMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);

        // 避免生成默认的part-m-00000等文件，因为，数据已经交给MultipleOutputs输出了
        LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);

        // 输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // 输出路径
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = outputPath.getFileSystem(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath);
        }
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
