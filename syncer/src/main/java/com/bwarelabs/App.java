package com.bwarelabs;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class App 
{
  public static void main(String[] args) throws IOException {
    // Bigtable Configuration
    Configuration config = BigtableConfiguration.configure("emulator", "solana-ledger");
    //config.set(BigtableOptionsFactory.BIGTABLE_EMULATOR_HOST_KEY, "localhost:8086");
    Connection connection = BigtableConfiguration.connect(config);
    Table table = connection.getTable(TableName.valueOf("blocks"));

    // Hadoop Configuration for SequenceFile

    Configuration hadoopConfig = new Configuration();
    Path path = new Path("file:///output/output.seq");
    Text key = new Text();
    LongWritable value = new LongWritable();
    RawLocalFileSystem fs = new RawLocalFileSystem();
    fs.setConf(hadoopConfig);
    SequenceFile.Writer writer = null;

    try {
      writer = SequenceFile.createWriter(hadoopConfig,
          SequenceFile.Writer.file(fs.makeQualified(path)),
          SequenceFile.Writer.keyClass(Text.class),
          SequenceFile.Writer.valueClass(LongWritable.class),
          SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));

      // Scan the table
      Scan scan = new Scan();
      ResultScanner scanner = table.getScanner(scan);
      for (Result result : scanner) {
        // Assuming you are storing the rows as 'long' values and the key as text
        key.set(new Text(result.getRow()));
        value.set(Bytes.toLong(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("qualifier"))));
        writer.append(key, value);
      }
    } finally {
      if (writer != null) {
        writer.close();
      }
      table.close();
      connection.close();
    }
  }
}
