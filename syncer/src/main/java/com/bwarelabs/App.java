// original backup
package com.bwarelabs;

import com.google.common.collect.Lists;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;

import org.apache.hadoop.io.SequenceFile.Writer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class App {
  public void testHBaseTypes() throws Exception {
    final List<KV<ImmutableBytesWritable, Result>> data = Lists.newArrayList();

    final int nRows = 10;
    for (int i = 0; i < nRows; i++) {
      String keyStr = String.format("%03d", i);

      ImmutableBytesWritable rowKey = new ImmutableBytesWritable(keyStr.getBytes());

      @SuppressWarnings("deprecation")
      Result value = Result.create(
          Collections.singletonList(
              CellUtil.createCell(
                  keyStr.getBytes(),
                  ("family").getBytes(),
                  ("qualifier" + i).getBytes(),
                  123456,
                  Type.Put.getCode(),
                  ("value" + i).getBytes())));
      data.add(KV.of(rowKey, value));
    }

    // Write the file
    // Configuration config = new Configuration(false);
    Configuration config = new Configuration();
    config.setStrings(
        "io.serializations",
        ResultSerialization.class.getName(),
        WritableSerialization.class.getName());

    RawLocalFileSystem fs = new RawLocalFileSystem();
    fs.setConf(config);
    Path path = new Path("file:///output/sequencefile/dummy_table.seq");

    Writer writer = null;
    try {
      writer = SequenceFile.createWriter(
          config,
          Writer.file(fs.makeQualified(path)),
          Writer.keyClass(ImmutableBytesWritable.class),
          Writer.valueClass(Result.class),
          Writer.compression(SequenceFile.CompressionType.NONE));

      for (KV<ImmutableBytesWritable, Result> kv : data) {
        writer.append(kv.getKey(), kv.getValue());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    if (writer != null) {
      System.out.println("Closing writer...");
      writer.close();
    }

  }

  public static void main(String[] args) throws IOException {
    // call testSimpleWritable
    App app = new App();
    System.out.println("Calling testHBaseTypes()...");
    try {
      app.testHBaseTypes();
    } catch (Exception e) {
      e.printStackTrace();
    }
    System.out.println("testHBaseTypes() done.");

    return;

    // Bigtable Configuration
    // Configuration config = BigtableConfiguration.configure("emulator",
    // "solana-ledger");
    // Connection connection = BigtableConfiguration.connect(config);
    // Table table = connection.getTable(TableName.valueOf("tx"));

    // // Hadoop Configuration for SequenceFile

    // Configuration hadoopConfig = new Configuration();

    // Path path = new Path("file:///output/sequencefile/tx.seq");
    // ImmutableBytesWritable key = new ImmutableBytesWritable();
    // Result value = new Result();

    // RawLocalFileSystem fs = new RawLocalFileSystem();
    // fs.setConf(hadoopConfig);
    // SequenceFile.Writer writer = null;

    // try {
    // writer = SequenceFile.createWriter(hadoopConfig,
    // SequenceFile.Writer.file(fs.makeQualified(path)),
    // SequenceFile.Writer.keyClass(Text.class),
    // SequenceFile.Writer.valueClass(Text.class),
    // SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));

    // Scan scan = new Scan();
    // ResultScanner scanner = table.getScanner(scan);

    // for (Result result : scanner) {
    // ImmutableBytesWritable rowKey = new ImmutableBytesWritable(result.getRow());

    // writer.append(rowKey, new ResultWritable(result));
    // }

    // } finally

    // {
    // if (writer != null) {
    // writer.close();
    // }
    // table.close();
    // connection.close();
    // }
  }
}
