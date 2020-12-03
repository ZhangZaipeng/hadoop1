package com.example.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HClient {

  public static void main(String[] args) throws IOException {
    Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", "hadoop6");
    configuration.set("hbase.zookeeper.property.clientPort", "2181");

    Connection connection = ConnectionFactory.createConnection(configuration);

    TableName userTableName = TableName.valueOf("user");

    Table table = connection.getTable(userTableName);

    Get get = new Get(Bytes.toBytes("524382618264914241"));

    Result result = table.get(get);

    Cell[] cells = result.rawCells();

    for (Cell cell : cells) {
      System.out.println("Row: " + new String(result.getRow()));
      System.out.println("Row: " + Bytes.toString(CellUtil.cloneRow(cell)));
      System.out.println("ColumFamily: " + Bytes.toString(CellUtil.cloneFamily(cell)));
      System.out.println("Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)));
      System.out.println("Value: " + Bytes.toString(CellUtil.cloneValue(cell)));
      System.out.println("TimeStamp:${cell.getTimestamp}");
      System.out.println("=======================================");
    }

    table.close();
    connection.close();
  }
}
