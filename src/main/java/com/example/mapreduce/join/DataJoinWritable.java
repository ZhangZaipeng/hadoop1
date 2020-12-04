package com.example.mapreduce.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataJoinWritable implements Writable {

    private String tag;
    private String data;

    public DataJoinWritable() {

    }
    public DataJoinWritable(String tag, String data) {
        this.set(tag,data);
    }
    public void set(String tag, String data) {
        this.tag = tag;
        this.data = data;
    }


    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.getTag());
        dataOutput.writeUTF(this.getData());
    }

    public void readFields(DataInput dataInput) throws IOException {

        this.setTag(dataInput.readUTF());
        this.setData(dataInput.readUTF());
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "DataJoinWritable{" +
                "tag='" + tag + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}
