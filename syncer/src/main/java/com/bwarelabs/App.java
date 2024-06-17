package com.bwarelabs;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;

import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.io.SequenceFile.Writer;

import java.io.IOException;
import java.util.Collections;

public class App {

  public static void main(String[] args) throws IOException {
    String readSource = null;
    boolean runChecksumOnHBase = false;

    // Parse command-line arguments
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
      app.writeSequenceFileFromBigTable(app);

      System.out.println("Done!");
      return;
    }

    if (readSource.equals("local-files")) {
      System.out.println("Reading data from local files from path '/input/storage'");

      app.writeSequenceFileFromLocalFiles(Paths.get("/input/storage"));

      System.out.println("Done!");
      return;
    }

    System.out.println("Error: Invalid 'read-source' argument. Valid values are 'bigtable' and 'local-files'.");
  }

  // Write SequenceFiles from Bigtable tables
  private void writeSequenceFileFromBigTable(App app) {
    // Bigtable Configuration
    Configuration config = BigtableConfiguration.configure("emulator",
            "solana-ledger");

    try (Connection connection = BigtableConfiguration.connect(config)) {
      app.writeSequenceFileFromTable(connection, "blocks");
      app.writeSequenceFileFromTable(connection, "entries");
      app.writeSequenceFileFromTable(connection, "tx");
      app.writeSequenceFileFromTable(connection, "tx-by-addr");
    } catch (IOException e) {
      e.printStackTrace();
    }
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
      writer = SequenceFile.createWriter(hadoopConfig,
              SequenceFile.Writer.file(fs.makeQualified(path)),
              SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
              SequenceFile.Writer.valueClass(Result.class),
              SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));

      Scan scan = new Scan();
      ResultScanner scanner = table.getScanner(scan);

      int numberOfRows = 0;
      for (Result result: scanner) {
        numberOfRows++;
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(result.getRow());
        writer.append(rowKey, result);

        String checksum = null;
        try {
          checksum = calculateSHA256Checksum(result);
        } catch (NoSuchAlgorithmException | IOException e) {
          e.printStackTrace();
        }
        System.out.println("Checksum for row " + numberOfRows + ": " + checksum);
      }

      System.out.println("Number of rows written: " + numberOfRows);

    } finally

    {
      if (writer != null) {
        System.out.println("Closing writer...");
        writer.close();
      }
      table.close();
    }
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
            checksum = calculateSHA256Checksum(result);
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

  private String calculateSHA256Checksum(Result result) throws NoSuchAlgorithmException, IOException {
    MessageDigest digest = MessageDigest.getInstance("SHA-256");
    CellScanner scanner = result.cellScanner();

    while (scanner.advance()) {
      byte[] value = scanner.current().getValueArray();
      int valueOffset = scanner.current().getValueOffset();
      int valueLength = scanner.current().getValueLength();
      digest.update(value, valueOffset, valueLength);
    }

    byte[] hashBytes = digest.digest();
    StringBuilder hexString = new StringBuilder();
    for (byte hashByte: hashBytes) {
      String hex = Integer.toHexString(0xff & hashByte);
      if (hex.length() == 1)
        hexString.append('0');
      hexString.append(hex);
    }
    return hexString.toString();
  }

  @SuppressWarnings("unused")
  private int calculateByteAdditionChecksum(Result result) throws IOException {
    int checksum = 0;
    CellScanner scanner = result.cellScanner();
    while (scanner.advance()) {
      byte[] value = scanner.current().getValueArray();
      for (byte b: value) {
        checksum += b;
      }

    }
    return checksum;
  }

  // Write SequenceFiles from local files
  private void writeSequenceFileFromLocalFiles(java.nio.file.Path inputDir) {
    try {
      Configuration hadoopConfig = new Configuration();
      hadoopConfig.setStrings("io.serializations", WritableSerialization.class.getName(), ResultSerialization.class.getName());


      java.nio.file.Path entriesPath = java.nio.file.Paths.get("/output/sequencefile/entries/entries.seq");
      java.nio.file.Path blocksPath = java.nio.file.Paths.get("/output/sequencefile/blocks/blocks.seq");
      java.nio.file.Path txPath = java.nio.file.Paths.get("/output/sequencefile/tx/tx.seq");
      java.nio.file.Path txByAddrPath = java.nio.file.Paths.get("/output/sequencefile/tx-by-addr/tx-by-addr.seq");


      RawLocalFileSystem fs = new RawLocalFileSystem();
      fs.setConf(hadoopConfig);
      Writer entriesWriter = null;
      Writer blocksWriter = null;
      Writer txWriter = null;
      Writer txByAddrWriter = null;


      try {
        entriesWriter = SequenceFile.createWriter(hadoopConfig,
                SequenceFile.Writer.file(new org.apache.hadoop.fs.Path(entriesPath.toUri())),
                SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
                SequenceFile.Writer.valueClass(Result.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));

        blocksWriter = SequenceFile.createWriter(hadoopConfig,
                SequenceFile.Writer.file(new org.apache.hadoop.fs.Path(blocksPath.toUri())),
                SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
                SequenceFile.Writer.valueClass(Result.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));

        txWriter = SequenceFile.createWriter(hadoopConfig,
                SequenceFile.Writer.file(new org.apache.hadoop.fs.Path(txPath.toUri())),
                SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
                SequenceFile.Writer.valueClass(Result.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));

        txByAddrWriter = SequenceFile.createWriter(hadoopConfig,
                SequenceFile.Writer.file(new org.apache.hadoop.fs.Path(txByAddrPath.toUri())),
                SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
                SequenceFile.Writer.valueClass(Result.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));


        Writer finalEntriesWriter = entriesWriter;
        Writer finalBlocksWriter = blocksWriter;
        Writer finalTxWriter = txWriter;
        Writer finalTxByAddrWriter = txByAddrWriter;

        Files.walkFileTree(inputDir, new SimpleFileVisitor < java.nio.file.Path > () {
          @Override
          public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) throws IOException {
            if (!Files.isDirectory(file)) {
              String filePath = file.toString();
              String[] pathParts = filePath.split("/");
              String fileName = pathParts[pathParts.length - 1];
              String folderName = pathParts[pathParts.length - 2];
              String rowKeyWithoutExtension = fileName.contains(".") ?
                      fileName.substring(0, fileName.lastIndexOf('.')) :
                      fileName;

              byte[] fileContent = Files.readAllBytes(file);
              ImmutableBytesWritable key = new ImmutableBytesWritable(rowKeyWithoutExtension.getBytes());
              long timestamp = System.currentTimeMillis();

              String columnFamily = "x";
              String qualifier;

              switch (folderName) {
                case "entries":
                case "blocks":
                case "tx_by_addr":
                  qualifier = "proto";
                  break;
                case "tx":
                  qualifier = "bin";
                  break;
                default:
                  System.out.println("Unknown folder type: " + folderName);
                  return FileVisitResult.CONTINUE;
              }

              // remove the termination from the file name
              String fileNameWithoutTermination = fileName.substring(0, fileName.lastIndexOf('.'));

              Cell cell = CellUtil.createCell(
                      Bytes.toBytes(fileNameWithoutTermination), // row key
                      Bytes.toBytes(columnFamily),
                      Bytes.toBytes(qualifier), // column qualifier
                      timestamp,
                      Cell.Type.Put.getCode(),
                      fileContent
              );

              Result result = Result.create(Collections.singletonList(cell));

              switch (folderName) {
                case "entries":
                  finalEntriesWriter.append(key, result);
                  System.out.println("Written to entries sequence file: " + filePath);
                  break;
                case "blocks":
                  finalBlocksWriter.append(key, result);
                  System.out.println("Written to blocks sequence file: " + filePath);
                  break;
                case "tx":
                  finalTxWriter.append(key, result);
                  System.out.println("Written to tx sequence file: " + filePath);
                  break;
                case "tx_by_addr":
                  finalTxByAddrWriter.append(key, result);
                  System.out.println("Written to tx_by_addr sequence file: " + filePath);
                  break;
              }
            }
            return FileVisitResult.CONTINUE;
          }
        });

      } finally {
        if (entriesWriter != null) {
          entriesWriter.close();
        }
        if (blocksWriter != null) {
          blocksWriter.close();
        }
        if (txWriter != null) {
          txWriter.close();
        }
        if (txByAddrWriter != null) {
          txByAddrWriter.close();
        }
      }
    } catch (IOException e) {
      System.out.println("Error: " + e.getMessage());
    }
  }
}