package com.jike.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class FlowCount {

    public static class TokenizerMapper
            extends Mapper<LongWritable, Text, LongWritable, FlowBean> {

        enum CountersEnum {INPUT_WORDS}

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1.获取数据
            String line = value.toString();

            //2.切分数据
            String[] fields = line.split("\t");

            //3.获取上传流量
            // 1363157993044 	18211575961	94-71-AC-CD-E6-18:CMCC-EASY	120.196.100.99	iface.qiyi.com	视频网站	15	12	1527	2106	200
            long upFlow = Long.parseLong(fields[fields.length - 3]);
            long downFlow = Long.parseLong(fields[fields.length - 2]);

            //4.输出
            context.write(new LongWritable(Long.parseLong(fields[1])), new FlowBean(upFlow, downFlow));

            Counter counter = context.getCounter(CountersEnum.class.getName(),
                    CountersEnum.INPUT_WORDS.toString());
            counter.increment(1);
        }
    }

    public static class CountReducer
            extends Reducer<LongWritable, FlowBean, LongWritable, FlowBean> {

        enum CountersEnum {REDUCER_WORDS}

        @Override
        public void reduce(LongWritable key, Iterable<FlowBean> values,
                           Context context) throws IOException, InterruptedException {
            long sumUp = 0;
            long sumDown = 0;
            for (FlowBean val : values) {
                sumUp += val.getUpFlow();
                sumDown += val.getDownFlow();
            }
            context.write(key, new FlowBean(sumUp, sumDown));
            Counter counter = context.getCounter(TokenizerMapper.CountersEnum.class.getName(),
                    CountReducer.CountersEnum.REDUCER_WORDS.toString());
            counter.increment(1);
        }
    }

    public static void main(String[] args) throws Exception {
        args = new String[]{WordCount.class.getClassLoader().getResource("HTTP_20130313143750.dat").getPath(),"./output"};

        //1.获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "xiejinjun_flow_count");

        //2.加载jar包
        job.setJarByClass(FlowCount.class);

        //3.关联map和reduce
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(CountReducer.class);
        job.setReducerClass(CountReducer.class);

        //4.设置最终输出类型
        // 这里类型一定要注意与前面一致
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FlowBean.class);

        //5.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6.提交job任务
        // job.waitForCompletion(true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}