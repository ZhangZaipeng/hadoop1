package com.example.mapreduce;

import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
        Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }


  public static void main(String[] args) throws Exception {

    // 1.configuration
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }


    // 2.建立job 创建本次mr程序的job实例
    Job job = Job.getInstance(conf, "word count");
    // 指定本次job运行的主类
    job.setJarByClass(WordCount.class);


    // 3.输入文件目录
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    // job.setInputFormatClass();


    // 5.map
    job.setMapperClass(TokenizerMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    // map第一阶段调用。Combiner的作用：1在map端对本地先做reduce功能。提高效率
    // job.setCombinerClass(IntSumReducer.class);
    // map第二阶段调用。Partitioner的作用：对key取hash值(或其它处理)，指定进入哪一个reduce
    // job.setPartitionerClass(FirstPartitioner.class);  // 设置自定分区
    // 同一分区中满足同组条件（可以是不同的key）的进入同一个Interator，执行一次reduce方法
    // job.setGroupingComparatorClass();
    // 每个分区内，对键或键的部分进行排序，保证分区内局部有序；
    // job.setSortComparatorClass();
    // job.setCombinerKeyGroupingComparatorClass();

    // reducer实现类
    job.setReducerClass(IntSumReducer.class);
    // 指定本次job reduce阶段的输出数据类型 也就是整个mr任务的最终输出类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);


    // 可指定 本地文件 or hdfs文件
    // job.setOutputFormatClass(); // 格式化输入文件
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));


    // 提交本次job
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
