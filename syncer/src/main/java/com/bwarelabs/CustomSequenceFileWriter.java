package com.bwarelabs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

public class CustomSequenceFileWriter {
    private final SequenceFile.Writer writer;
    private final FSDataOutputStream fsDataOutputStream;

    public CustomSequenceFileWriter(Configuration conf, FSDataOutputStream out) throws IOException {
        this.fsDataOutputStream = out;
        this.writer = SequenceFile.createWriter(conf,
                SequenceFile.Writer.stream(out),
                SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
                SequenceFile.Writer.valueClass(Result.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
    }

    public void append(ImmutableBytesWritable key, Result value) throws IOException {
        this.writer.append(key, value);
    }

    public void close() throws IOException {
        System.out.println("Closing custom sequence file writer");
        this.writer.close();

        if (fsDataOutputStream != null) {
            fsDataOutputStream.close();
        }
    }
}
