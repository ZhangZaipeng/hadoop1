package com.example.mapreduce.dept;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Description ：
 * @Tauthor ZhangZaipeng
 * @Tdata 2020/7/24   10:37
 */
public class Q2_DeptNumberAveSalary extends Configured implements Tool {

  public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
    private String[] kv;

    // 此方法会在Map方法执行之前执行且执行一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {

      }
    }

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      // 对员工文件字段进行拆分
      kv = value.toString().split(",");
      // map join: 在map阶段过滤掉不需要的数据，输出key为部门名称和value为员工工资
      if (null != kv[5] && !"".equals(kv[5])) {
        context.write(new Text(kv[7].trim()), new Text(kv[5].trim()));
      }
    }
  }

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      // 对同一部门的员工工资进行求和
      long sumSalary = 0;
      long sumDept = 0;
      for (Text val : values) {
        sumSalary += Long.parseLong(val.toString());
        sumDept ++;
      }
      long avgSalary = sumSalary / sumDept;
      // 输出key为部门名称和value为该部门员工工资总和
      context.write(key, new Text("Dept Number:" + sumDept +
          " sum Salary::" + sumSalary + "avgSalary:" + avgSalary));
    }
  }

  public int run(String[] strings) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), strings).getRemainingArgs();

    // 2.建立job 创建本次mr程序的job实例
    Job job = Job.getInstance(getConf(), "Q2_DeptNumberAveSalary");
    // 指定本次job运行的主类
    job.setJarByClass(Q2_DeptNumberAveSalary.class);
    // 3.输入文件目录
    job.setInputFormatClass(TextInputFormat.class); // 格式化输入文件
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }

    // 5.map
    job.setMapperClass(Q2_DeptNumberAveSalary.MapClass.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
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
    job.setReducerClass(Q2_DeptNumberAveSalary.Reduce.class);
    // 指定本次job reduce阶段的输出数据类型 也就是整个mr任务的最终输出类型
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);


    // 可指定 本地文件 or hdfs文件
    // job.setOutputFormatClass(); // 格式化输入文件
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

    job.waitForCompletion(true);

    return job.isSuccessful() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new Q2_DeptNumberAveSalary(), args);
    System.exit(res);
  }

}
