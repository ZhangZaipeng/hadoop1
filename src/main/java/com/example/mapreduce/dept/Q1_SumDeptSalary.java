package com.example.mapreduce.dept;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * @Description ：
 * @Tauthor ZhangZaipeng
 * @Tdata 2020/7/23   14:21
 */
public class Q1_SumDeptSalary extends Configured implements Tool {

  private static Logger logger = Logger.getLogger(Q1_SumDeptSalary.class);

  public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

    // 用于缓存 dept文件中的数据
    private Map<String, String> deptMap = new HashMap<String, String>();

    private String[] kv;

    // 此方法会在Map方法执行之前执行且执行一次
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      super.setup(context);
      if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
        BufferedReader bufferedReader = null;
        FileReader fileReader = null;
        try {
          URI[] uris = context.getCacheFiles();
          String deptIdName = null;
          for (URI uri : uris) {
            String path = uri.getPath();
            fileReader = new FileReader(path);
            bufferedReader = new BufferedReader(fileReader);
            while (null != (deptIdName = bufferedReader.readLine())) {
              // 对部门文件字段进行拆分并缓存到deptMap中
              // 其中Map中key为部门编号，value为所在部门名称
              deptMap.put(deptIdName.split(",")[0], deptIdName.split(",")[1]);
            }
          }
        } catch (IOException e) {
          e.printStackTrace();
        } finally {
          try {
            if (bufferedReader != null) {
              bufferedReader.close();
            }
            if (fileReader != null) {
              fileReader.close();
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }

      }
    }

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      // 对员工文件字段进行拆分
      kv = value.toString().split(",");
      // map join: 在map阶段过滤掉不需要的数据，输出key为部门名称和value为员工工资
      /*if (deptMap.containsKey(kv[7])) {
        if (null != kv[5] && !"".equals(kv[5])) {
          context.write(new Text(deptMap.get(kv[7].trim())), new Text(kv[5].trim()));
        }
      }*/
      if (null != kv[5] && !"".equals(kv[5])) {
        context.write(new Text(kv[7].trim()), new Text(kv[5].trim()));
      }
    }
  }


  public static class Reduce extends Reducer<Text, Text, Text, LongWritable> {
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      // 对同一部门的员工工资进行求和
      long sumSalary = 0;
      for (Text val : values) {
        sumSalary += Long.parseLong(val.toString());
      }
      // 输出key为部门名称和value为该部门员工工资总和
      context.write(key, new LongWritable(sumSalary));
    }
  }

  public int run(String[] strings) throws Exception {
    // 实例化作业对象，设置作业名称、Mapper和Reduce类
    Job job = Job.getInstance(getConf(), "Q1SumDeptSalary");
    job.setJobName("Q1SumDeptSalary");

    job.setJarByClass(Q1_SumDeptSalary.class);
    job.setMapperClass(MapClass.class);
    job.setReducerClass(Reduce.class);

    // 设置输入格式类
    job.setInputFormatClass(TextInputFormat.class);

    // 设置输出格式
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // 第1个参数为缓存的部门数据路径、第2个参数为员工数据路径和第3个参数为输出路径

    String[] otherArgs = new GenericOptionsParser(job.getConfiguration(), strings).getRemainingArgs();
    job.addCacheFile(new URI(otherArgs[0]));
    FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

    job.waitForCompletion(true);

    return job.isSuccessful() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new Q1_SumDeptSalary(), args);
    System.exit(res);
  }

}
