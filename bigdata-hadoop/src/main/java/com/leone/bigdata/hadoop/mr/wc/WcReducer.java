package com.leone.bigdata.hadoop.mr.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * <p>
 *
 * @author leone
 * @since 2018-11-21
 **/
public class WcReducer extends Reducer<Text, IntWritable, Text, LongWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        Iterator<IntWritable> iterator = values.iterator();
        for (IntWritable count : values) {
            i += count.get();
        }
        context.write(key, new LongWritable(i));
    }

}
