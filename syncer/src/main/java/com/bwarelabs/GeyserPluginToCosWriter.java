package com.bwarelabs;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.PutObjectRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class GeyserPluginToCosWriter {
    private static final String BUCKET_NAME = "geyser-plugin";

    public static void write() {
        writeSequenceFileFromLocalFiles(java.nio.file.Paths.get("/input/storage"));
    }

    private static void writeSequenceFileFromLocalFiles(java.nio.file.Path inputDir) {
        try {
            Configuration hadoopConfig = new Configuration();
            hadoopConfig.setStrings("io.serializations", WritableSerialization.class.getName(), ResultSerialization.class.getName());

            List<String> paths = new ArrayList<>();
            paths.add("entries");
            paths.add("blocks");
            paths.add("tx");
            paths.add("tx_by_addr");

            Path entriesPath = new Path("file:///output/sequencefile/entries/entries.seq");
            final SequenceFile.Writer entriesWriter = createWriter(hadoopConfig, entriesPath);

            Path blocksPath = new Path("file:///output/sequencefile/blocks/blocks.seq");
            final SequenceFile.Writer blocksWriter = createWriter(hadoopConfig, blocksPath);

            Path txPath = new Path("file:///output/sequencefile/tx/tx.seq");
            final SequenceFile.Writer txWriter = createWriter(hadoopConfig, txPath);

            Path txByAddrPath = new Path("file:///output/sequencefile/tx_by_addr/tx_by_addr.seq");
            final SequenceFile.Writer txByAddrWriter = createWriter(hadoopConfig, txByAddrPath);

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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    private static CompletableFuture<Void> uploadFileToCOS(org.apache.hadoop.fs.Path filePath) {
//        return CompletableFuture.runAsync(() -> {
//            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
//                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
//                            "http://cos.ap-guangzhou.myqcloud.com",
//                            "ap-guangzhou"))
//                    .build();
//
//            String key = filePath.getFileName().toString();
//
//            try {
//                s3Client.putObject(new PutObjectRequest(BUCKET_NAME, key, new File(filePath.toString())));
//                System.out.println("Uploaded file to COS: " + filePath);
//            } catch (Exception e) {
//                System.out.println("Error uploading file to COS: " + filePath + " - " + e.getMessage());
//            }
//        });
//    }

    private static SequenceFile.Writer createWriter(Configuration hadoopConfig, Path path) {
        try {
            return SequenceFile.createWriter(hadoopConfig,
                    SequenceFile.Writer.file(path),
                    SequenceFile.Writer.keyClass(ImmutableBytesWritable.class),
                    SequenceFile.Writer.valueClass(Result.class),
                    SequenceFile.Writer.compression(SequenceFile.CompressionType.NONE));
        } catch (IOException e) {
            throw new RuntimeException("Error creating sequence file writer", e);
        }
    }
}
