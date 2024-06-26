package com.bwarelabs;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.serializer.WritableSerialization;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

public class BigTableToSequenceFileWriter {
    private final Connection connection;

    public BigTableToSequenceFileWriter() {
        Configuration configuration = BigtableConfiguration.configure("emulator", "solana-ledger");

        connection = BigtableConfiguration.connect(configuration);
    }

    public void write() {
        try {
            writeSequenceFileFromTable(connection, "blocks");
            writeSequenceFileFromTable(connection, "entries");
            writeSequenceFileFromTable(connection, "tx");
            writeSequenceFileFromTable(connection, "tx-by-addr");
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
        SequenceFile.Writer writer = null;

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
                    checksum = Utils.calculateSHA256Checksum(result);
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

}
