package com.example.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataJoinMR extends Configured implements Tool {

  /**
   * map TODO
   */
  public static class TempleteMapper extends Mapper<LongWritable, Text, Text, DataJoinWritable> {

    private Text outputKey = new Text();
    private DataJoinWritable outputValue = new DataJoinWritable();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      //TODO
    }

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] values = value.toString().split(",");
      if ((3 != values.length) && (4 != values.length)) {
        return;
      }

      //customer
      if (3 == values.length) {
        String cid = values[0];
        String name = values[1];
        String telphone = values[2];
        outputKey.set(cid);
        outputValue.set(DataCommon.CUSTOMER, name + "," + telphone);
      }

      //order
      if (4 == values.length) {
        String cid = values[1];
        String price = values[2];
        String productName = values[3];
        outputKey.set(cid);
        outputValue.set(DataCommon.ORDER, productName + "," + price);
      }

      context.write(outputKey, outputValue);

    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      //TODO
    }
  }


  /**
   * reduce TODO
   */
  public static class TempleteReduce extends Reducer<Text, DataJoinWritable, NullWritable, Text> {

    private Text outputValue = new Text();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      //TODO
    }

    @Override
    public void reduce(Text key, Iterable<DataJoinWritable> values, Context context)
        throws IOException, InterruptedException {

      // <cid,List(customerInfo,orderInfo,orderInfo,orderInfo)>
      String customerInfo = null;
      List<String> orderList = new ArrayList<String>();

      for (DataJoinWritable dataJoinWritable : values) {
        if (DataCommon.CUSTOMER.equals(dataJoinWritable.getTag())) {
          customerInfo = dataJoinWritable.getData();
        } else if (DataCommon.ORDER.equals(dataJoinWritable.getTag())) {
          orderList.add(dataJoinWritable.getData());
        }
      }

      for (String orderInfo : orderList) {
        if (customerInfo == null) {
          continue;
        }
        outputValue.set(key.toString() + "," + customerInfo + "," + orderInfo);
        context.write(NullWritable.get(), outputValue);
      }

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

    job.setMapperClass(TempleteMapper.class);
    //TODO
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DataJoinWritable.class);

    //1.分区
    //job.setPartitionerClass();

    //2.排序
    //job.setSortComparatorClass();

    //3.combiner
    //job.setCombinerClass(WordCountCombiner.class);

    //4.compress
    //configuration.set("mapreduce.map.output.compress","true");
    //configuration.set("mapreduce.map.output.compress.codec","org.apache.hadoop.io.compress.SnappyCodec");

    //5.分组
    //job.setGroupingComparatorClass();

    //3.3) reduce
    job.setReducerClass(TempleteReduce.class);
    //TODO
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    //job.setNumReduceTasks(2);

    //3.4) output
    Path output = new Path(args[1]);
    FileOutputFormat.setOutputPath(job, output);

    //4) commit
    boolean isSuc = job.waitForCompletion(true);
    return (isSuc) ? 0 : 1;
  }

  public static void main(String[] args) {

    args = new String[]{
        "hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/datas",
        "hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/mr/output"
    };

    Configuration configuration = new Configuration();
    try {

      //先判断
      Path fileOutPath = new Path(args[1]);
      FileSystem fileSystem = FileSystem.get(configuration);
      if (fileSystem.exists(fileOutPath)) {
        fileSystem.delete(fileOutPath, true);
      }

      //int status = wordCountMR.run(args);

      int status = ToolRunner.run(configuration, new DataJoinMR(), args);
      System.exit(status);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
