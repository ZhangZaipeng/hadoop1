package com.example.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WebPV extends Configured implements Tool {

  /**
   * map TODO
   */
  public static class WebPVMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    private final static IntWritable mapOutValue = new IntWritable(1);
    private IntWritable mapOutKey = new IntWritable();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      //TODO
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] values = value.toString().split("\t");

      if (30 > values.length) {
        context.getCounter("WEBPV_COUNTERS", "LENGTH_LT30_COUNTER").increment(1);
        return;
      }
      String provinceIdValue = values[23];
      String url = values[1];

      if (StringUtils.isBlank(provinceIdValue)) {
        context.getCounter("WEBPV_COUNTERS", "PROVINCEID_ISBLACK_COUNTER").increment(1);
        return;
      }
      if (StringUtils.isBlank(url)) {
        context.getCounter("WEBPV_COUNTERS", "URL_ISBLACK_COUNTER").increment(1);
        return;
      }

      int provinceId = 0;
      try {
        provinceId = Integer.valueOf(provinceIdValue);
      } catch (Exception e) {
        context.getCounter("WEBPV_COUNTERS", "PROVINCEID_VALIDATE_COUNTER").increment(1);
        return;
      }

      mapOutKey.set(provinceId);
      context.write(mapOutKey, mapOutValue);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      //TODO
    }

  }


  /**
   * reduce TODO
   */
  public static class WebPVReduce extends
      Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    private IntWritable outputValue = new IntWritable();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      //TODO
    }

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable value : values) {
        sum += value.get();
      }
      outputValue.set(sum);
      context.write(key, outputValue);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      //TODO
    }
  }


  /**
   * run
   */
  public int run(String args[]) throws Exception {

    //driver
    //1) get conf
    Configuration configuration = this.getConf();

    //2) create job
    Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
    job.setJarByClass(this.getClass());

    //3.1)  input
    Path path = new Path(args[0]);
    FileInputFormat.addInputPath(job, path);

    //3.2) map

    job.setMapperClass(WebPVMapper.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    //1.分区
    //job.setPartitionerClass();

    //2.排序
    //job.setSortComparatorClass();

    //3.combiner
    job.setCombinerClass(WebPVReduce.class);

    //4.compress
    //configuration.set("mapreduce.map.output.compress","true");
    //configuration.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

    //5.分组
    //job.setGroupingComparatorClass();

    //3.3) reduce
    job.setReducerClass(WebPVReduce.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    //job.setNumReduceTasks(0);

    //job.setNumReduceTasks(2);

    //3.4) output
    Path output = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, output);

    //4) commit
    boolean isSuc = job.waitForCompletion(true);
    return (isSuc) ? 0 : 1;
  }

  public static void main(String[] args) {

//        args = new String[]{
//         "hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/datas/webpv/",
//         "hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/mr/output"
//        };

    Configuration configuration = new Configuration();
    try {

      //先判断
      Path fileOutPath = new Path(args[1]);
      FileSystem fileSystem = FileSystem.get(configuration);
      if (fileSystem.exists(fileOutPath)) {
        fileSystem.delete(fileOutPath, true);
      }

      //int status = wordCountMR.run(args);

      int status = ToolRunner.run(configuration, new WebPV(), args);
      System.exit(status);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
