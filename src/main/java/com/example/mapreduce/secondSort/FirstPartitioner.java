package com.example.mapreduce.secondSort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class FirstPartitioner extends Partitioner<PairWritable,IntWritable> {
    @Override
    public int getPartition(PairWritable key, IntWritable intWritable, int i) {
        return (key.getFirst().hashCode() & 2147483647) % i;
    }
}
