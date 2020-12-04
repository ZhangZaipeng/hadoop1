package com.example.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileInputStream;

public class HdfsAPP {

  private FileSystem getFileSystem() throws Exception {
    Configuration configuration = new Configuration();
    configuration.set("fs.default.name", "hdfs://hadoop1:8020");

    FileSystem fileSystem = FileSystem.get(configuration);
    return fileSystem;
  }

  private void readHDFSFile(String filePath) {
    FSDataInputStream fsDataInputStream = null;
    try {
      Path path = new Path(filePath);
      fsDataInputStream = this.getFileSystem().open(path);
      IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);

    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (fsDataInputStream != null) {
        IOUtils.closeStream(fsDataInputStream);
      }
    }
  }


  private void writeHDFS(String localPath, String hdfsPath) {
    FSDataOutputStream outputStream = null;
    FileInputStream fileInputStream = null;
    try {
      Path path = new Path(hdfsPath);
      outputStream = this.getFileSystem().create(path);
      fileInputStream = new FileInputStream(new File(localPath));
      IOUtils.copyBytes(fileInputStream, outputStream, 4096, false);
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (fileInputStream != null) {
        IOUtils.closeStream(fileInputStream);
      }
      if (outputStream != null) {
        IOUtils.closeStream(outputStream);
      }

    }
  }

  public static void main(String[] args) {
    HdfsAPP hdfsAPP = new HdfsAPP();
    //String filePath = "hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/hdfs/core-site.xml";
    String hdfsPath = "hdfs://bigdata-pro-m01.kfk.com:9000/user/kfk/hdfs/local.xml";
    String localPath = "/Users/caojinbo/Documents/workspace/src/main/resources/hdfs-site.xml";

    hdfsAPP.readHDFSFile("/hdfs/input/wordcount/1.txt");
    // hdfsAPP.writeHDFS(localPath, hdfsPath);
  }
}
