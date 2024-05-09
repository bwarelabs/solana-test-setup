package com.bwarelabs;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

public class App {
    public static void main(String[] args) throws IOException {
        // Bigtable Configuration
        Configuration config = BigtableConfiguration.configure("emulator", "solana-ledger");
        Connection connection = BigtableConfiguration.connect(config);
        Table table = connection.getTable(TableName.valueOf("tx"));

        // Hadoop Configuration for SequenceFile

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
                + "org.apache.hadoop.io.serializer.WritableSerialization");

        Path path = new Path("file:///sequencefile/output.seq");
        ImmutableBytesWritable key = new ImmutableBytesWritable();
        Result value = new Result();

        RawLocalFileSystem fs = new RawLocalFileSystem();
        fs.setConf(hadoopConfig);
        SequenceFile.Writer writer = null;

        try {
            writer = SequenceFile.createWriter(hadoopConfig,
                    SequenceFile.Writer.file(fs.makeQualified(path)),
                    SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
                    SequenceFile.Writer.valueClass(Result.class),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));

            // Scan the table
            Scan scan = new Scan();
            ResultScanner scanner = table.getScanner(scan);

            int numberOfRows = 0;
            for (Result result : scanner) {
                key.set(result.getRow());
                writer.append(key, result);
            }

            System.out.println("Number of rows: " + numberOfRows);

        } finally {
            if (writer != null) {
                writer.close();
            }
            table.close();
            connection.close();
        }
    }
}
