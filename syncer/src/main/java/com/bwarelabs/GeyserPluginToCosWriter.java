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
    private static final String BUCKET_NAME = "bwaresolanatest-1322657745";
    private static final String COS_ENDPOINT = "http://cos.ap-chengdu.myqcloud.com";
    private static final String REGION = "ap-chengdu";
    private static final String AWS_ID_KEY = "IKIDKWVM51sW5RhmgQzMbdS5XnqgR1Wr0ykT";
    private static final String AWS_SECRET_KEY = "mUPKBsOoANTZVFn8IW0PzVp2Jqz3vb5M";

    private static final S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
            .endpointOverride(URI.create(COS_ENDPOINT))
            .region(Region.of(REGION))
            .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(AWS_ID_KEY, AWS_SECRET_KEY)))
            .build();

    private static final S3TransferManager transferManager = S3TransferManager.builder()
            .s3Client(s3AsyncClient)
            .build();

    public static void write() {
        System.out.println("Starting write process...");

        try {
            // List slot range directories
            Files.list(Paths.get("/input/storage"))
                    .filter(Files::isDirectory)
                    .forEach(slotRangeDir -> processSlotRange(slotRangeDir));

            System.out.println("All slot ranges processed and uploaded.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processSlotRange(Path slotRangeDir) {
        System.out.println("Processing slot range: " + slotRangeDir.getFileName());

        try {
            Configuration hadoopConfig = new Configuration();
            hadoopConfig.setStrings("io.serializations", WritableSerialization.class.getName(), ResultSerialization.class.getName());

            // Create writers for each category within this slot range
            CustomS3FSDataOutputStream entriesStream = new CustomS3FSDataOutputStream(slotRangeDir, "entries");
            CustomSequenceFileWriter entriesWriter = createWriter(hadoopConfig, entriesStream);

            CustomS3FSDataOutputStream blocksStream = new CustomS3FSDataOutputStream(slotRangeDir, "blocks");
            CustomSequenceFileWriter blocksWriter = createWriter(hadoopConfig, blocksStream);

            CustomS3FSDataOutputStream txStream = new CustomS3FSDataOutputStream(slotRangeDir, "tx");
            CustomSequenceFileWriter txWriter = createWriter(hadoopConfig, txStream);

            CustomS3FSDataOutputStream txByAddrStream = new CustomS3FSDataOutputStream(slotRangeDir, "tx_by_addr");
            CustomSequenceFileWriter txByAddrWriter = createWriter(hadoopConfig, txByAddrStream);

            Files.list(slotRangeDir)
                    .filter(Files::isDirectory)
                    .forEach(slotDir -> {
                        try {
                            processSlot(slotDir, entriesWriter, blocksWriter, txWriter, txByAddrWriter);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });

            entriesWriter.close();
            blocksWriter.close();
            txWriter.close();
            txByAddrWriter.close();

            waitForAsyncUploads(entriesStream, blocksStream, txStream, txByAddrStream);
            System.out.println("Slot range processed: " + slotRangeDir.getFileName());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processSlot(Path slotDir, CustomSequenceFileWriter entriesWriter, CustomSequenceFileWriter blocksWriter, CustomSequenceFileWriter txWriter, CustomSequenceFileWriter txByAddrWriter) throws IOException {
        Files.walkFileTree(slotDir, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
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

                Cell cell = CellUtil.createCell(
                        rowKeyWithoutExtension.getBytes(),
                        Bytes.toBytes(columnFamily),
                        Bytes.toBytes(qualifier),
                        timestamp,
                        Cell.Type.Put.getCode(),
                        fileContent
                );

                Result result = Result.create(Collections.singletonList(cell));

                switch (folderName) {
                    case "entries":
                        entriesWriter.append(key, result);
                        break;
                    case "blocks":
                        blocksWriter.append(key, result);
                        break;
                    case "tx":
                        txWriter.append(key, result);
                        break;
                    case "tx_by_addr":
                        txByAddrWriter.append(key, result);
                        break;
                }
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static CustomSequenceFileWriter createWriter(Configuration conf, FSDataOutputStream outputStream) throws IOException {
        return new CustomSequenceFileWriter(conf, outputStream);
    }

    private static void waitForAsyncUploads(CustomS3FSDataOutputStream... streams) {
        System.out.println("Waiting for all asynchronous uploads to complete...");
        CompletableFuture<Void> allUploads = CompletableFuture.allOf(
                streams[0].getUploadFuture(),
                streams[1].getUploadFuture(),
                streams[2].getUploadFuture(),
                streams[3].getUploadFuture()
        );

        allUploads.join();
        System.out.println("All asynchronous uploads have completed.");
    }

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

    private static class CustomS3FSDataOutputStream extends FSDataOutputStream {
        private final ByteArrayOutputStream buffer;
        private final String s3Key;
        private CompletableFuture<CompletedUpload> uploadFuture;

        public CustomS3FSDataOutputStream(Path slotRangeDir, String category) throws IOException {
            this(new ByteArrayOutputStream(), slotRangeDir, category);
            System.out.println("CustomS3FSDataOutputStream created for key: " + s3Key);
        }

        private CustomS3FSDataOutputStream(ByteArrayOutputStream buffer, Path slotRangeDir, String category) {
            super(buffer, null);
            this.buffer = buffer;
            this.s3Key = slotRangeDir.getFileName() + "/sequencefile/" + category + "/" + category + ".seq";
        }

        @Override
        public void close() throws IOException {
            System.out.println("Closing stream for: " + s3Key);
            super.close();
            byte[] content = buffer.toByteArray();
            System.out.println("Uploading " + s3Key + " to S3 asynchronously");

            try {
                PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key(s3Key)
                        .build();

                UploadRequest uploadRequest = UploadRequest.builder()
                        .putObjectRequest(putObjectRequest)
                        .requestBody(AsyncRequestBody.fromBytes(content))
                        .addTransferListener(LoggingTransferListener.create())
                        .build();

                Upload upload = transferManager.upload(uploadRequest);

                this.uploadFuture = upload.completionFuture();

                this.uploadFuture.whenComplete((completedUpload, throwable) -> {
                    if (throwable != null) {
                        throwable.printStackTrace();
                    } else {
                        System.out.println("Successfully uploaded " + s3Key + " to S3");
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Failed to upload " + s3Key + " to S3");
            } finally {
                buffer.close();
            }
        }

        public CompletableFuture<CompletedUpload> getUploadFuture() {
            return uploadFuture;
        }
    }
}
