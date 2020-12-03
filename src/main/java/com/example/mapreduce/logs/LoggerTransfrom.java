package com.example.mapreduce.logs;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Description ：
 * @Tauthor ZhangZaipeng
 * @Tdata 2020/8/18   20:19
 */
public class LoggerTransfrom extends Configured implements Tool {

  public static class TransfromMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private String reg = "^(2018|2019|2020)-[0-1]\\d-[1-3]\\d-[0-2]\\d-[0-5]\\d-[0-5]\\d$";

    private SimpleDateFormat ft =
        new SimpleDateFormat("yyyy-MM-dd-hh-mm-ss");


    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      //通过计数器实现计数统计
      context.getCounter("Counter", "总计数据").increment(1);

      String line = value.toString();
      String[] strings = StringUtils.split(line, ',');
      if (strings.length == 6) {
        String dataString = strings[0];
        if (dataString.matches(reg)) { // 匹配
          context.getCounter("Counter", "合法数据").increment(1);
        } else {
          context.getCounter("Counter", "合法数据-转换").increment(1);
          strings[0] = ft.format(new Date());
        }

        StringBuffer bf = new StringBuffer();
        for (String word : strings) {
          bf.append(word).append(",");
        }
        bf.deleteCharAt(bf.length() - 1);

        context.write(key, new Text(bf.toString()));
      } else {
        context.getCounter("Counter", "非法数据").increment(1);
      }
    }
  }

  public static class TransfromReduce extends Reducer<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      // super.reduce(key, values, context);
      for (Text value : values) {
        context.write(value, NullWritable.get());
      }
    }
  }

  @Override
  public int run(String[] otherArgs) throws Exception {
    // 实例化作业对象，设置作业名称、Mapper和Reduce类
    Job job = Job.getInstance(getConf(), "LoggerTransfrom");

    job.setJarByClass(LoggerTransfrom.class);

    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    job.setInputFormatClass(TextInputFormat.class); // 格式化输入文件

    job.setMapperClass(TransfromMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(TransfromReduce.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

    job.waitForCompletion(true);

    return job.isSuccessful() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    int res = ToolRunner.run(conf, new LoggerTransfrom(), args);
    System.exit(res);
  }

}
