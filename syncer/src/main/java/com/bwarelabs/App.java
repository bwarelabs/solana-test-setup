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
import java.util.Arrays;

public class App {

  public static void main(String[] args) {
    String readSource = null;
    String bigtableTable = null;
    String[] validBigtableTables = {"blocks", "entries", "tx", "tx-by-addr"};

    for (String arg : args) {
      if (arg.startsWith("read-source=")) {
        readSource = arg.split("=")[1];
      } else if (readSource != null && readSource.equals("bigtable") && bigtableTable == null) {
        bigtableTable = arg;
      }
    }

    if (readSource == null) {
      System.out.println("Error: 'read-source' argument is required.");
      System.out.println("Valid values for 'read-source' are 'bigtable' and 'local-files'.");
      return;
    }

    if (readSource.equals("bigtable")) {
      if (bigtableTable == null || !Arrays.asList(validBigtableTables).contains(bigtableTable)) {
        System.out.println("Error: When 'read-source' is 'bigtable', a second argument must be provided with one of the following values: 'blocks', 'entries', 'tx', 'tx-by-addr'.");
        return;
      }
      System.out.println("Writing SequenceFiles from Bigtable table: " + bigtableTable);
      BigTableToCosWriter bigTableToCosWriter = new BigTableToCosWriter();
      bigTableToCosWriter.write(bigtableTable);

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
  @SuppressWarnings("unused")
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