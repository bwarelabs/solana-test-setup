package com.bwarelabs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.serializer.WritableSerialization;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

public class GeyserPluginToCosWriter {
    private static final String BUCKET_NAME = "bwaresolanatest-1322657745";
    private static final String COS_ENDPOINT = "https://cos.ap-chengdu.myqcloud.com";
    private static final String REGION = "ap-chengdu";
    private static final String AWS_ID_KEY = "IKIDKWVM51sW5RhmgQzMbdS5XnqgR1Wr0ykT";
    private static final String AWS_SECRET_KEY = "mUPKBsOoANTZVFn8IW0PzVp2Jqz3vb5M";

    private static final S3Client s3Client = S3Client.builder()
            .endpointOverride(URI.create(COS_ENDPOINT))
            .region(Region.of(REGION))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(AWS_ID_KEY, AWS_SECRET_KEY)))
            .build();

    public static void write() {
        System.out.println("Starting write process...");
        writeSequenceFileFromLocalFiles(java.nio.file.Paths.get("/input/storage"));
    }

    private static void writeSequenceFileFromLocalFiles(java.nio.file.Path inputDir) {
        try {
            Configuration hadoopConfig = new Configuration();
            hadoopConfig.setStrings("io.serializations", WritableSerialization.class.getName(), ResultSerialization.class.getName());


            CustomS3FSDataOutputStream entriesStream = new CustomS3FSDataOutputStream("sequencefile/entries/entries.seq");
            final CustomSequenceFileWriter entriesWriter = createWriter(hadoopConfig, entriesStream);


            CustomS3FSDataOutputStream blocksStream = new CustomS3FSDataOutputStream("sequencefile/blocks/blocks.seq");
            final CustomSequenceFileWriter blocksWriter = createWriter(hadoopConfig, blocksStream);


            CustomS3FSDataOutputStream txStream = new CustomS3FSDataOutputStream("sequencefile/tx/tx.seq");
            final CustomSequenceFileWriter txWriter = createWriter(hadoopConfig, txStream);


            CustomS3FSDataOutputStream txByAddrStream = new CustomS3FSDataOutputStream("sequencefile/tx_by_addr/tx_by_addr.seq");
            final CustomSequenceFileWriter txByAddrWriter = createWriter(hadoopConfig, txByAddrStream);


            Files.walkFileTree(inputDir, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) throws IOException {
                    if (Files.isDirectory(file)) {
                        return FileVisitResult.CONTINUE;
                    }

                    String filePath = file.toString();
                    String[] pathParts = filePath.split("/");
                    String fileName = pathParts[pathParts.length - 1];
                    String folderName = pathParts[pathParts.length - 2];
                    String rowKeyWithoutExtension = fileName.contains(".") ?
                            fileName.substring(0, fileName.lastIndexOf('.')) :
                            fileName;

                    if (folderName.equals("tx_by_addr")) {
                              if (fileName.startsWith("Vote")) {
                                System.out.println("Skipping file: " + fileName);
                                return FileVisitResult.CONTINUE;
                              }
                        rowKeyWithoutExtension = rowKeyWithoutExtension.replace("_", "/");
                    }

                    byte[] fileContent = Files.readAllBytes(file);
                    ImmutableBytesWritable key = new ImmutableBytesWritable(rowKeyWithoutExtension.getBytes());
                    long timestamp = System.currentTimeMillis(); // todo hbase timestamp

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

                    Cell cell = CellUtil.createCell(
                            rowKeyWithoutExtension.getBytes(), // row key
                            Bytes.toBytes(columnFamily),
                            Bytes.toBytes(qualifier), // column qualifier
                            timestamp,
                            Cell.Type.Put.getCode(),
                            fileContent
                    );

                    Result result = Result.create(Collections.singletonList(cell));

                    switch (folderName) {
                        case "entries":
                            entriesWriter.append(key, result);
//                            System.out.println("Written to entries sequence file: " + filePath);
                            break;
                        case "blocks":
                            blocksWriter.append(key, result);
//                            System.out.println("Written to blocks sequence file: " + filePath);
                            break;
                        case "tx":
                            txWriter.append(key, result);
//                            System.out.println("Written to tx sequence file: " + filePath);
                            break;
                        case "tx_by_addr":
                            txByAddrWriter.append(key, result);
//                            System.out.println("Written to tx-by-addr sequence file: " + filePath);
                            break;
                    }
                    return FileVisitResult.CONTINUE;
                }
            });

            entriesWriter.close();
            blocksWriter.close();
            txWriter.close();
            txByAddrWriter.close();

            System.out.println("Finished writing sequence files to COS");

            holdProgramForDebugging();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static CustomSequenceFileWriter createWriter(Configuration conf, FSDataOutputStream outputStream) throws IOException {
        return new CustomSequenceFileWriter(conf, outputStream);
    }

    private static void holdProgramForDebugging() {
        System.out.println("Hold the program to observe the logs. Press Enter to exit.");
        try {
            // Using a scanner to wait for user input before exiting
            Scanner scanner = new Scanner(System.in);
            scanner.nextLine();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class CustomS3FSDataOutputStream extends FSDataOutputStream {
        private final ByteArrayOutputStream buffer;
        private final String s3Key;

        public CustomS3FSDataOutputStream(String s3Key) throws IOException {
            this(new ByteArrayOutputStream(), s3Key);
            System.out.println("CustomS3FSDataOutputStream created for key: " + s3Key);
        }

        private CustomS3FSDataOutputStream(ByteArrayOutputStream buffer, String s3Key) {
            super(buffer, null);
            this.buffer = buffer;
            this.s3Key = s3Key;
        }

        @Override
        public void close() throws IOException {
            System.out.println("Closing stream for: " + s3Key);
            super.close();
            byte[] content = buffer.toByteArray();
            System.out.println("Uploading " + s3Key + " to S3 synchronously");

            try {
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key(s3Key)
                        .build();

                s3Client.putObject(putObjectRequest, RequestBody.fromBytes(content));
                System.out.println("Successfully uploaded " + s3Key + " to S3");
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Failed to upload " + s3Key + " to S3");
            } finally {
                buffer.close();
            }
        }
    }

    // Wrapper class for SequenceFile.Writer
    private static class CustomSequenceFileWriter {
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
            writer.append(key, value);
        }

        public void close() throws IOException {
            System.out.println("CustomSequenceFileWriter close called");
            writer.close();
            // for some reason, using fsDataOutputStream didn't call the close method of CustomS3FSDataOutputStream
            fsDataOutputStream.close();
        }
    }

}
