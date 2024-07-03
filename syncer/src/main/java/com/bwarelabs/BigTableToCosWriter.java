package com.bwarelabs;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class BigTableToCosWriter {
    private static final Logger logger = Logger.getLogger(BigTableToCosWriter.class.getName());

    private final Connection connection;
    private final ExecutorService executorService;
    private final Semaphore batchSemaphore;
    private static final int THREAD_COUNT = 16;
    private static final int ROWS_PER_THREAD = 1500;
    private static final int MAX_CONCURRENT_BATCHES = 100;
    private final List<CompletableFuture<Void>> allUploadFutures = new ArrayList<>();

    public BigTableToCosWriter() {
        try {
            LogManager.getLogManager().readConfiguration(
                    BigTableToCosWriter.class.getClassLoader().getResourceAsStream("logging.properties"));
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Could not setup logger configuration", e);
        }

        Configuration configuration = BigtableConfiguration.configure("emulator", "solana-ledger");
        connection = BigtableConfiguration.connect(configuration);
        executorService = Executors.newFixedThreadPool(THREAD_COUNT);
        batchSemaphore = new Semaphore(MAX_CONCURRENT_BATCHES);
    }

    public void write() {
        logger.info("Starting BigTable to COS writer");
        List<String> tables = List.of("blocks", "entries", "tx", "tx-by-addr");

        for (String table : tables) {
            try {
                processTable(table);
                logger.info("Processing table: " + table);
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error processing table: " + table, e);
            }
        }

        CompletableFuture<Void> allUploads = CompletableFuture.allOf(allUploadFutures.toArray(new CompletableFuture[0]));
        allUploads.thenRun(() -> logger.info("All tables processed and uploaded."))
                .join();

        executorService.shutdown();
    }

    private void processTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        scan.setCaching(ROWS_PER_THREAD);
        ResultScanner scanner = table.getScanner(scan);

        int startRow = 0;
        List<Result> batch = new ArrayList<>(ROWS_PER_THREAD);
        for (Result result : scanner) {
            batch.add(result);
            if (batch.size() == ROWS_PER_THREAD) {
                int endRow = startRow + ROWS_PER_THREAD;
                submitBatch(tableName, startRow, endRow, new ArrayList<>(batch));
                batch.clear();
                startRow = endRow;
            }
        }
        if (!batch.isEmpty()) {
            int endRow = startRow + batch.size();
            submitBatch(tableName, startRow, endRow, batch);
        }

        table.close();
    }

    private void submitBatch(String tableName, int startRow, int endRow, List<Result> batch) {
        CompletableFuture.runAsync(() -> {
            try {
                batchSemaphore.acquire();
                writeBatchToCos(tableName, startRow, endRow, batch)
                        .exceptionally(ex -> {
                            logger.log(Level.SEVERE, "Error writing batch to COS", ex);
                            return null;
                        })
                        .whenComplete((v, ex) -> batchSemaphore.release());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.log(Level.SEVERE, "Batch processing interrupted", e);
            }
        }, executorService);
    }

    private CompletableFuture<Void> writeBatchToCos(String tableName, int startRow, int endRow, List<Result> batch) {
        logger.info(String.format("[%s] Writing batch for %s from %d to %d", Thread.currentThread().getName(), tableName, startRow, endRow));

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.setStrings(
                "io.serializations",
                ResultSerialization.class.getName(),
                WritableSerialization.class.getName());

        RawLocalFileSystem fs = new RawLocalFileSystem();
        fs.setConf(hadoopConfig);
        CustomS3FSDataOutputStream customFSDataOutputStream;
        try {
            customFSDataOutputStream = new CustomS3FSDataOutputStream(Paths.get("output/sequencefile/" + tableName + "/range_" + startRow + "_" + endRow), tableName);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error creating CustomS3FSDataOutputStream", e);
            return CompletableFuture.failedFuture(e);
        }

        try (CustomSequenceFileWriter customWriter = new CustomSequenceFileWriter(hadoopConfig, customFSDataOutputStream)) {
            for (Result result : batch) {
                ImmutableBytesWritable rowKey = new ImmutableBytesWritable(result.getRow());
                customWriter.append(rowKey, result);
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error writing batch to CustomSequenceFileWriter", e);
            return CompletableFuture.failedFuture(e);
        }

        try {
            customFSDataOutputStream.close();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error closing CustomS3FSDataOutputStream", e);
            return CompletableFuture.failedFuture(e);
        }

        CompletableFuture<CompletedUpload> uploadFuture = customFSDataOutputStream.getUploadFuture();
        allUploadFutures.add(uploadFuture.thenAccept(completedUpload -> {
            logger.info(String.format("[%s] Successfully uploaded %s to COS", Thread.currentThread().getName(), customFSDataOutputStream.getS3Key()));
        }));

        return uploadFuture.thenAccept(completedUpload -> {});
    }
}
