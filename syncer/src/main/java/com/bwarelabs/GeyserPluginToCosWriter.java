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
import org.apache.hadoop.fs.Path;

import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;
import software.amazon.awssdk.transfer.s3.model.UploadRequest;
import software.amazon.awssdk.transfer.s3.model.Upload;
import software.amazon.awssdk.transfer.s3.progress.LoggingTransferListener;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

public class GeyserPluginToCosWriter {
    private static final String BUCKET_NAME = "bucket3-1250000000";
    private static final String COS_ENDPOINT = "http://cos.ap-guangzhou.myqcloud.com"; // Replace with your COS endpoint
    private static final String REGION = "ap-guangzhou";

    private static final S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
            .endpointOverride(URI.create(COS_ENDPOINT))
            .region(Region.of(REGION))
//            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(AWS_ACCESS_KEY, AWS_SECRET_KEY)))
            .build();
    private static final S3TransferManager transferManager = S3TransferManager.builder()
            .s3Client(s3AsyncClient)
            .build();


//    AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
//            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
//                    "http://cos.ap-guangzhou.myqcloud.com",
//                    "ap-guangzhou"))
//            .build();



    public static void write() {
        writeSequenceFileFromLocalFiles(java.nio.file.Paths.get("/input/storage"));
    }

    private static void writeSequenceFileFromLocalFiles(java.nio.file.Path inputDir) {
        try {
            Configuration hadoopConfig = new Configuration();
            hadoopConfig.setStrings("io.serializations", WritableSerialization.class.getName(), ResultSerialization.class.getName());


//            Path entriesPath = new Path("file:///output/sequencefile/entries/entries.seq");
//            final SequenceFile.Writer entriesWriter = createWriter(hadoopConfig, entriesPath);
            S3OutputStream entriesStream = new S3OutputStream("sequencefile/entries/entries.seq");
            final SequenceFile.Writer entriesWriter = createWriter(hadoopConfig, entriesStream);


//            Path blocksPath = new Path("file:///output/sequencefile/blocks/blocks.seq");
//            final SequenceFile.Writer blocksWriter = createWriter(hadoopConfig, blocksPath);
            S3OutputStream blocksStream = new S3OutputStream("sequencefile/blocks/blocks.seq");
            final SequenceFile.Writer blocksWriter = createWriter(hadoopConfig, blocksStream);


//            Path txPath = new Path("file:///output/sequencefile/tx/tx.seq");
//            final SequenceFile.Writer txWriter = createWriter(hadoopConfig, txPath);
            S3OutputStream txStream = new S3OutputStream("sequencefile/tx/tx.seq");
            final SequenceFile.Writer txWriter = createWriter(hadoopConfig, txStream);


//            Path txByAddrPath = new Path("file:///output/sequencefile/tx_by_addr/tx_by_addr.seq");
//            final SequenceFile.Writer txByAddrWriter = createWriter(hadoopConfig, txByAddrPath);
            S3OutputStream txByAddrStream = new S3OutputStream("sequencefile/tx_by_addr/tx_by_addr.seq");
            final SequenceFile.Writer txByAddrWriter = createWriter(hadoopConfig, txByAddrStream);


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
                            System.out.println("Written to entries sequence file: " + filePath);
                            break;
                        case "blocks":
                            blocksWriter.append(key, result);
                            System.out.println("Written to blocks sequence file: " + filePath);
                            break;
                        case "tx":
                            txWriter.append(key, result);
                            System.out.println("Written to tx sequence file: " + filePath);
                            break;
                        case "tx_by_addr":
                            txByAddrWriter.append(key, result);
                            System.out.println("Written to tx-by-addr sequence file: " + filePath);
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

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static SequenceFile.Writer createWriter(Configuration conf, FSDataOutputStream outputStream) throws IOException {
        return SequenceFile.createWriter(conf,
                SequenceFile.Writer.stream(outputStream),
                SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
                SequenceFile.Writer.valueClass(Result.class),
                SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
    }

    private static class S3OutputStream extends FSDataOutputStream {
        private final ByteArrayOutputStream buffer;
        private final String s3Key;

        public S3OutputStream(String s3Key) {
            super(null, null);
            this.buffer = new ByteArrayOutputStream();
            this.s3Key = s3Key;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            buffer.write(b, off, len);
        }

        @Override
        public void write(int b) throws IOException {
            buffer.write(b);
        }

        @Override
        public void close() throws IOException {
            super.close();
            byte[] content = buffer.toByteArray();
            CompletableFuture<CompletedUpload> uploadFuture = uploadToS3Async(s3Key, content);

            uploadFuture.whenComplete((completedUpload, throwable) -> {
                if (throwable != null) {
                    throwable.printStackTrace();
                } else {
                    System.out.println("Successfully uploaded " + s3Key + " to S3");
                }
            });
            buffer.close();
        }


        private CompletableFuture<CompletedUpload> uploadToS3Async(String key, byte[] content) {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(BUCKET_NAME)
                    .key(key)
                    .build();

            UploadRequest uploadRequest = UploadRequest.builder()
                    .putObjectRequest(putObjectRequest)
                    .requestBody(AsyncRequestBody.fromBytes(content))
                    .addTransferListener(LoggingTransferListener.create())
                    .build();

            Upload upload = transferManager.upload(uploadRequest);
            return upload.completionFuture();
        }
    }
}
