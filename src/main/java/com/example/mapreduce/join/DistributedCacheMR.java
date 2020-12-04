package com.example.mapreduce.join;

import jdk.nashorn.internal.scripts.JO;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class DistributedCacheMR extends Configured implements Tool{

    static  Map<String,String> customerMap = new HashMap<String,String>();
    /**
     * map
     * TODO
     *
     */
    public static class TempleteMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Text outputKey = new Text();
        private Text outputValue = new Text();

        @Override
        public void setup(Context context) throws IOException, InterruptedException {

            Configuration configuration = context.getConfiguration();
            URI[] uri = Job.getInstance(configuration).getCacheFiles();
            Path path = new Path(uri[0]);
            FileSystem fileSystem = FileSystem.get(configuration);
            InputStream inputStream = fileSystem.open(path);

            InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String line = null;
            while ((line = bufferedReader.readLine() ) != null){
                if(line.trim().length() > 0) {
                    customerMap.put(line.split(",")[0],line);

                }
            }

            bufferedReader.close();;
            inputStreamReader.close();
            inputStream.close();

        }

        @Override
        public  void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            //100,1,45.50,product-1
            String lineValue = value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(lineValue,",");
            while (stringTokenizer.hasMoreTokens()){
                String wordValue = stringTokenizer.nextToken();
                if(customerMap.get(wordValue) != null){

                    outputKey.set(wordValue);
                    outputValue.set(customerMap.get(wordValue) + lineValue);
                    context.write(outputKey,outputValue);
                    break;
                }
            }

        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            //TODO
        }


    }


    /**
     * reduce
     * TODO
     */
    public static class TempleteReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
             //TODO
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //TODO
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            //TODO
        }


    }


    /**
     * run
     * @param args
     * @return
     * @throws Exception
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
        job.setMapOutputValueClass(Text.class);

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
        //job.setReducerClass(TempleteReduce.class);
        //TODO
        //job.setOutputKeyClass(Text.class);
       // job.setOutputValueClass(IntWritable.class);

       ///job.setNumReduceTasks(2);

        URI uri = new URI(args[2]);
        job.addCacheFile(uri);
        //3.4) output
        Path output = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, output);

        //4) commit
        boolean isSuc = job.waitForCompletion(true);
        return (isSuc) ? 0 : 1;
    }

    public static void main(String[] args) {


        args = new String[]{
         "hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/datas/order.txt",
        "hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/mr/output",
         "hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/datas/custom.txt"
        };


        Configuration configuration = new Configuration();
        try {

            //先判断
            Path fileOutPath = new Path(args[1]);
            FileSystem fileSystem = FileSystem.get(configuration);
            if(fileSystem.exists(fileOutPath)){
                fileSystem.delete(fileOutPath,true);
            }

            //int status = wordCountMR.run(args);

            int status = ToolRunner.run(configuration,new DistributedCacheMR(),args);
            System.exit(status);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
