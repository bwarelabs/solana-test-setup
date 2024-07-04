package com.bwarelabs;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.ResultSerialization;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.serializer.WritableSerialization;
import software.amazon.awssdk.transfer.s3.model.CompletedUpload;

import java.io.IOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private final Map<String, String> checkpoints = new HashMap<>();
    private static final String CHECKPOINT_FILE = "checkpoints.txt";

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
        loadCheckpoints();
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
        allUploads.join();

        executorService.shutdown();
        logger.info("All tables processed and uploaded.");
    }

    private void processTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        Scan scan = new Scan();
        scan.setCaching(ROWS_PER_THREAD);

        int startRow = 0;

        if (checkpoints.containsKey(tableName)) {
            String startRowKey = checkpoints.get(tableName);
            scan.withStartRow(Bytes.toBytes(startRowKey));
            if (tableName.equals("blocks") || tableName.equals("entries")) {
                startRow = getRowNumberFromKey(startRowKey);
            }
        }

        ResultScanner scanner = table.getScanner(scan);

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
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            try {
                batchSemaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.log(Level.SEVERE, "Batch processing interrupted", e);
            }
        }, executorService).thenCompose(v ->
                writeBatchToCos(tableName, startRow, endRow, batch)
                        .whenComplete((result, ex) -> {
                            batchSemaphore.release();
                            if (ex == null) {
                                updateCheckpoint(tableName, batch.get(batch.size() - 1).getRow());
                            } else {
                                logger.log(Level.SEVERE, "Error writing batch to COS", ex);
                            }
                        })
        );

        allUploadFutures.add(future);
    }

    private CompletableFuture<Void> writeBatchToCos(String tableName, int startRow, int endRow, List<Result> batch) {
        logger.info(String.format("[%s] Converting batch to sequencefile format for %s from %d to %d", Thread.currentThread().getName(), tableName, startRow, endRow));

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
        return uploadFuture.thenAccept(completedUpload -> {
            logger.info(String.format("[%s] Successfully queued %s upload to COS", Thread.currentThread().getName(), customFSDataOutputStream.getS3Key()));
        }).thenRun(() -> {});
    }

    private void updateCheckpoint(String tableName, byte[] lastProcessedRow) {
        checkpoints.put(tableName, Bytes.toString(lastProcessedRow));
        saveCheckpoints();
    }

    private void saveCheckpoints() {
        try {
            List<String> lines = new ArrayList<>();
            for (Map.Entry<String, String> entry : checkpoints.entrySet()) {
                lines.add(entry.getKey() + "=" + entry.getValue());
            }
            Files.write(Paths.get(CHECKPOINT_FILE), lines);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error saving checkpoints", e);
        }
    }

    private void loadCheckpoints() {
        try {
            Path checkpointPath = Paths.get(CHECKPOINT_FILE);
            if (Files.exists(checkpointPath)) {
                List<String> lines = Files.readAllLines(checkpointPath);
                for (String line : lines) {
                    String[] parts = line.split("=");
                    if (parts.length == 2) {
                        checkpoints.put(parts[0], parts[1]);
                    }
                }
            }
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error loading checkpoints", e);
        }
    }

    private int getRowNumberFromKey(String rowKey) {
        try {
            return Integer.parseInt(rowKey, 16);
        } catch (NumberFormatException e) {
            logger.log(Level.WARNING, "Invalid row key format: " + rowKey, e);
            return 0;
        }
    }
}
