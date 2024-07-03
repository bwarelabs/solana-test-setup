package com.bwarelabs;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;

import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;

import java.io.IOException;

public class App {

  public static void main(String[] args) throws IOException {
    String readSource = null;
    boolean runChecksumOnHBase = false;

    for (String arg: args) {
      if (arg.startsWith("read-source=")) {
        readSource = arg.split("=")[1];
      } else if (arg.equals("run-checksum-on-hbase")) {
        runChecksumOnHBase = true;
      }
    }

    if ((readSource != null && runChecksumOnHBase) || (readSource == null && !runChecksumOnHBase)) {
      System.out.println("Error: Specify either 'read-source' or 'run-checksum-on-hbase', but not both.");
      return;
    }

    App app = new App();

    if (readSource == null) {
      // the files need to be imported into HDFS before running the
      // readDataAndCalculateChecksum

      System.out.println("Running checksum calculation on HBase tables.");
      app.readDataAndCalculateChecksum("blocks");
      app.readDataAndCalculateChecksum("entries");
      app.readDataAndCalculateChecksum("tx");
      app.readDataAndCalculateChecksum("tx-by-addr");

      System.out.println("Done!");
      return;
    }

    if (readSource.equals("bigtable")) {
      System.out.println("Writing SequenceFiles from Bigtable tables.");
      BigTableToCosWriter bigTableToCosWriter = new BigTableToCosWriter();
      bigTableToCosWriter.write();

      System.out.println("Done!");
      return;
    }

    if (readSource.equals("local-files")) {
      System.out.println("Reading data from local files from path '/input/storage'");

      GeyserPluginToCosWriter.watchDirectory(Path.of("/input/storage"));

      System.out.println("Done!");
      return;
    }

    System.out.println("Error: Invalid 'read-source' argument. Valid values are 'bigtable' and 'local-files'.");
  }

  // Read data from HBase tables and calculate checksum
  private void readDataAndCalculateChecksum(String tableName) throws IOException {
    System.out.println("Reading data from table: " + tableName);

    org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();
    config.set("hbase.zookeeper.quorum", "hbase");
    config.set("hbase.zookeeper.property.clientPort", "2181");

    try (Connection connection = ConnectionFactory.createConnection(config); Table table = connection.getTable(TableName.valueOf(tableName))) {

      Scan scan = new Scan();
      int numberOfRows = 0;
      try (ResultScanner scanner = table.getScanner(scan)) {
        for (Result result: scanner) {

          String checksum = null;
          try {
            checksum = Utils.calculateSHA256Checksum(result);
          } catch (NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
          }
          numberOfRows++;

          System.out.println("Checksum for row " + numberOfRows + ": " + checksum);
        }
      }
      System.out.println("Number of rows read: " + numberOfRows);
    }
  }
}