package com.bwarelabs;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.ChecksumUtil;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;

import org.apache.hadoop.io.SequenceFile.Writer;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class App {

  public static void main(String[] args) throws IOException {

    // Bigtable Configuration
    Configuration config = BigtableConfiguration.configure("emulator",
        "solana-ledger");
    Connection connection = BigtableConfiguration.connect(config);

    App app = new App();
    try {
      app.writeSequenceFileFromTable(connection, "blocks");
      // app.writeSequenceFileFromTable(connection, "entries");
      // app.writeSequenceFileFromTable(connection, "tx");
      // app.writeSequenceFileFromTable(connection, "tx-by-addr");

      app.readDataAndCalculateChecksum("blocks");
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      connection.close();
    }

    System.out.println("Done!");
    return;
  }

  private void writeSequenceFileFromTable(Connection connection, String tableName) throws IOException {
    System.out.println("Writing SequenceFile from table: " + tableName);

    Table table = connection.getTable(TableName.valueOf(tableName));

    // Hadoop Configuration for SequenceFile
    Configuration hadoopConfig = new Configuration();
    hadoopConfig.setStrings(
        "io.serializations",
        ResultSerialization.class.getName(),
        WritableSerialization.class.getName());

    Path path = new Path("file:///output/sequencefile/" + tableName + "/" + tableName + ".seq");

    RawLocalFileSystem fs = new RawLocalFileSystem();
    fs.setConf(hadoopConfig);
    Writer writer = null;

    try {
      // writer = SequenceFile.createWriter(hadoopConfig,
      // SequenceFile.Writer.file(fs.makeQualified(path)),
      // SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
      // SequenceFile.Writer.valueClass(Result.class),
      // SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));

      Scan scan = new Scan();
      ResultScanner scanner = table.getScanner(scan);

      int numberOfRows = 0;
      for (Result result : scanner) {
        numberOfRows++;
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(result.getRow());
        // writer.append(rowKey, result);

        int checksum = calculateByteAdditionChecksum(result);
        System.out.println("Checksum for row " + numberOfRows + ": " + checksum);
      }

      System.out.println("Number of rows written: " + numberOfRows);

    } finally

    {
      if (writer != null) {
        System.out.println("Closing writer...");
        // writer.close();
      }
      table.close();
    }
  }

  private int calculateByteAdditionChecksum(Result result) throws IOException {
    int checksum = 0;
    CellScanner scanner = result.cellScanner();
    while (scanner.advance()) {
      byte[] value = scanner.current().getValueArray();
      for (byte b : value) {
        checksum += b;
      }

    }
    return checksum;
  }

  private void readDataAndCalculateChecksum(String tableName) throws IOException {
    System.out.println("Reading data from table: " + tableName);
    // Set up HBase configuration
    org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
    config.set("hbase.zookeeper.quorum", "hbase");
    config.set("hbase.zookeeper.property.clientPort", "2181");

    try (Connection connection = ConnectionFactory.createConnection(config);
        Table table = connection.getTable(TableName.valueOf(tableName))) {

      Scan scan = new Scan();
      int numberOfRows = 0;
      try (ResultScanner scanner = table.getScanner(scan)) {
        for (Result result : scanner) {
          int checksum = calculateByteAdditionChecksum(result);
          System.out.println("Checksum for row: " + checksum);

          numberOfRows++;
        }
      }
      System.out.println("Number of rows read: " + numberOfRows);
    }
  }
}
