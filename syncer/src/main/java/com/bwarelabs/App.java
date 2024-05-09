// original backup
package com.bwarelabs;

import com.google.common.collect.Lists;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.apache.hadoop.io.SequenceFile.Writer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class App {
  public void testSimpleWritable() throws IOException {
    Configuration config = new Configuration(false);

    List<KV<Text, Text>> data = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      data.add(KV.of(new Text("key" + i), new Text("value" + i)));
    }

    File targetFile = new File("file:///output/sequencefile/dummy_table");

    try {
      Writer writer = SequenceFile.createWriter(
          config,
          Writer.file(new org.apache.hadoop.fs.Path(targetFile.toString())),
          Writer.keyClass(ImmutableBytesWritable.class),
          Writer.valueClass(Text.class));

      for (KV<Text, Text> kv : data) {
        // writer.append(kv.getKey(), kv.getValue());
        writer.append(new ImmutableBytesWritable(kv.getKey().getBytes()), kv.getValue());
        System.out.println("Wrote: " + kv.getKey() + ", " + kv.getValue());
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void testHBaseTypes() throws Exception {
    File targetFile = new File("file:///output/sequencefile/dummy_table");

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

    // // Read the file
    // SequenceFileSource<ImmutableBytesWritable, Result> source = new
    // SequenceFileSource<>(
    // StaticValueProvider.of(targetFile.getAbsolutePath()),
    // ImmutableBytesWritable.class,
    // WritableSerialization.class,
    // Result.class,
    // ResultSerialization.class,
    // SequenceFile.SYNC_INTERVAL);

    // // Verify
    // List<KV<ImmutableBytesWritable, Result>> actual =
    // SourceTestUtils.readFromSource(source, null);
    // assertThat(actual, hasSize(data.size()));

    // Collections.sort(
    // actual,
    // new Comparator<KV<ImmutableBytesWritable, Result>>() {
    // @Override
    // public int compare(
    // KV<ImmutableBytesWritable, Result> o1, KV<ImmutableBytesWritable, Result> o2)
    // {
    // return o1.getKey().compareTo(o2.getKey());
    // }
    // });
    // for (int i = 0; i < data.size(); i++) {
    // KV<ImmutableBytesWritable, Result> expectedKv = data.get(i);
    // KV<ImmutableBytesWritable, Result> actualKv = actual.get(i);

    // assertEquals(expectedKv.getKey(), actualKv.getKey());
    // assertEquals(actualKv.getValue().rawCells().length,
    // expectedKv.getValue().rawCells().length);

    // for (int j = 0; j < expectedKv.getValue().rawCells().length; j++) {
    // Cell expectedCell = expectedKv.getValue().rawCells()[j];
    // Cell actualCell = actualKv.getValue().rawCells()[j];
    // assertTrue(CellUtil.equals(expectedCell, actualCell));
    // assertTrue(CellUtil.matchingValue(expectedCell, actualCell));
    // }
    // }
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
