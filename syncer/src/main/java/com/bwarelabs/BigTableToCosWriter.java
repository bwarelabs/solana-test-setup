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
import java.math.BigInteger;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class BigTableToCosWriter {
    private static final Logger logger = Logger.getLogger(BigTableToCosWriter.class.getName());

    private final Connection connection;
    private final ExecutorService executorService;
    private static final int THREAD_COUNT = 2;
    private static final int SUBRANGE_SIZE = 4; // Number of rows to process in each batch within a thread range
    private static final int BATCH_LIMIT = 5;  // Limit the number of chained batches
    private final List<CompletableFuture<Void>> allUploadFutures = new ArrayList<>();
    private final Map<Integer, String> checkpoints = new HashMap<>();

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
        loadCheckpoints();
    }

    public void write() {
        logger.info("Starting BigTable to COS writer");

        List<String[]> hexRanges = splitHexRange();
        List<String[]> txRanges = splitRangeTx();

        List<String> tables = List.of("blocks");

        for (int i = 0; i < THREAD_COUNT; i++) {
            String[] hexRange = hexRanges.get(i);
            String[] txRange = txRanges.get(i);

            for (String table : tables) {
                String startRow = checkpoints.getOrDefault(i, table.equals("tx") || table.equals("tx-by-addr") ? txRange[0] : hexRange[0]);
                String endRow = table.equals("tx") || table.equals("tx-by-addr") ? txRange[1] : hexRange[1];

                logger.info(String.format("Table: %s, Range: %s - %s", table, startRow, endRow));
                processTableRange(i, table, startRow, endRow);
            }
        }

        CompletableFuture<Void> allUploads = CompletableFuture.allOf(allUploadFutures.toArray(new CompletableFuture[0]));
        allUploads.join();

        executorService.shutdown();
        logger.info("All tables processed and uploaded.");
    }

    private void processTableRange(int threadId, String tableName, String startRowKey, String endRowKey) {
        CompletableFuture<Void> processingFuture = CompletableFuture.runAsync(() -> {
            try {
                String currentStartRow = startRowKey;
                List<CompletableFuture<Void>> batchFutures = new ArrayList<>();

                while (compareKeys(currentStartRow, endRowKey) <= 0) {
                    for (int i = 0; i < BATCH_LIMIT && compareKeys(currentStartRow, endRowKey) <= 0; i++) {
                        String currentEndRow = calculateEndRow(currentStartRow, endRowKey);
                        List<Result> batch = fetchBatch(tableName, currentStartRow, currentEndRow);

                        CustomS3FSDataOutputStream customFSDataOutputStream = convertToSequenceFile(tableName, currentStartRow, currentEndRow, batch);

                        final String startRowForLogging = currentStartRow;
                        final String endRowForLogging = currentEndRow;
                        System.out.println(String.format("[%s] Processing batch %s - %s", Thread.currentThread().getName(), startRowForLogging, endRowForLogging));

                        CompletableFuture<Void> uploadFuture = customFSDataOutputStream.getUploadFuture()
                                .thenRun(() -> {
                                    System.out.println(String.format("[%s] Successfully uploaded %s to COS", Thread.currentThread().getName(), customFSDataOutputStream.getS3Key()));
                                });

                        batchFutures.add(uploadFuture);

                        currentStartRow = incrementRowKey(currentEndRow);
                    }

                    CompletableFuture<Void> currentBatch = CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0]));
                    currentBatch.join();

                    updateCheckpoint(threadId, currentStartRow);

                    batchFutures.clear();
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error processing table range for " + tableName, e);
            }
        }, executorService);

        allUploadFutures.add(processingFuture);
    }

    private String calculateEndRow(String startRow, String endRow) {
        long start = Long.parseUnsignedLong(startRow, 16);
        long end = Long.parseUnsignedLong(endRow, 16);
        long rangeSize = Math.min(SUBRANGE_SIZE, end - start + 1);
        long newEnd = start + rangeSize - 1;
        return String.format("%016x", newEnd);
    }

    private String incrementRowKey(String rowKey) {
        long row = Long.parseUnsignedLong(rowKey, 16);
        return String.format("%016x", row + 1);
    }

    private int compareKeys(String key1, String key2) {
        return Long.compareUnsigned(Long.parseUnsignedLong(key1, 16), Long.parseUnsignedLong(key2, 16));
    }

    private List<Result> fetchBatch(String tableName, String startRowKey, String endRowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        scan.withStartRow(Bytes.toBytes(startRowKey));
        scan.withStopRow(Bytes.toBytes(endRowKey));
        scan.setCaching(SUBRANGE_SIZE);

        ResultScanner scanner = table.getScanner(scan);
        List<Result> batch = new ArrayList<>(SUBRANGE_SIZE);

        for (Result result : scanner) {
            batch.add(result);
        }

        table.close();
        return batch;
    }

    private CustomS3FSDataOutputStream convertToSequenceFile(String tableName, String startRowKey, String endRowKey, List<Result> batch) throws IOException {
        logger.info(String.format("[%s] Converting batch to sequencefile format for %s from %s to %s", Thread.currentThread().getName(), tableName, startRowKey, endRowKey));

        Configuration hadoopConfig = new Configuration();
        hadoopConfig.setStrings(
                "io.serializations",
                ResultSerialization.class.getName(),
                WritableSerialization.class.getName());

        RawLocalFileSystem fs = new RawLocalFileSystem();
        fs.setConf(hadoopConfig);
        CustomS3FSDataOutputStream customFSDataOutputStream = new CustomS3FSDataOutputStream(Paths.get("output/sequencefile/" + tableName + "/range_" + startRowKey + "_" + endRowKey), tableName);

        CustomSequenceFileWriter customWriter = new CustomSequenceFileWriter(hadoopConfig, customFSDataOutputStream);

        for (Result result : batch) {
            ImmutableBytesWritable rowKey = new ImmutableBytesWritable(result.getRow());
            customWriter.append(rowKey, result);
        }

        customWriter.close();

        return customFSDataOutputStream;
    }

    private void updateCheckpoint(int threadId, String endRowKey) {
        checkpoints.put(threadId, endRowKey);
        saveCheckpoint(threadId, endRowKey);
    }

    private void saveCheckpoint(int threadId, String endRowKey) {
        try {
            Files.write(Paths.get("checkpoint_" + threadId + ".txt"), endRowKey.getBytes());
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error saving checkpoint for thread " + threadId, e);
        }
    }

    private void loadCheckpoints() {
        for (int i = 0; i < THREAD_COUNT; i++) {
            Path checkpointPath = Paths.get("checkpoint_" + i + ".txt");
            if (Files.exists(checkpointPath)) {
                try {
                    String checkpoint = new String(Files.readAllBytes(checkpointPath));
                    checkpoints.put(i, checkpoint);
                } catch (IOException e) {
                    logger.log(Level.SEVERE, "Error loading checkpoint for thread " + i, e);
                }
            }
        }
    }

    public static List<String[]> splitHexRange() {
        BigInteger start = new BigInteger("0000000000000000", 16);
        BigInteger end = new BigInteger("0000000000000211", 16);

        // Calculate the size of each interval
        BigInteger totalRange = end.subtract(start).add(BigInteger.ONE); // +1 to include the end in the range
        BigInteger intervalSize = totalRange.divide(BigInteger.valueOf(THREAD_COUNT));
        BigInteger remainder = totalRange.mod(BigInteger.valueOf(THREAD_COUNT));

        List<String[]> intervals = new ArrayList<>();
        BigInteger currentStart = start;

        for (int i = 0; i < THREAD_COUNT; i++) {
            BigInteger currentEnd = currentStart.add(intervalSize).subtract(BigInteger.ONE);

            if (remainder.compareTo(BigInteger.ZERO) > 0) {
                currentEnd = currentEnd.add(BigInteger.ONE);
                remainder = remainder.subtract(BigInteger.ONE);
            }

            intervals.add(new String[]{
                    formatHex(currentStart),
                    formatHex(currentEnd)
            });

            currentStart = currentEnd.add(BigInteger.ONE);
        }

        return intervals;
    }

    private static String formatHex(BigInteger value) {
        return String.format("%016x", value);
    }

    private List<String[]> splitRangeTx() {
        // Custom logic to split non-hex keys, e.g., by using the hash or other criteria.
        // This example assumes keys are split based on their hash values.
        List<String[]> ranges = new ArrayList<>();
        // Assuming we can get some min and max hash values for tx keys
        long start = 0; // Replace with actual start if known
        long end = Long.MAX_VALUE; // Replace with actual end if known
        long rangeSize = (end - start) / THREAD_COUNT;
        for (int i = 0; i < THREAD_COUNT; i++) {
            long rangeStart = start + i * rangeSize;
            long rangeEnd = (i == THREAD_COUNT - 1) ? end : rangeStart + rangeSize - 1;
            ranges.add(new String[]{Long.toString(rangeStart), Long.toString(rangeEnd)});
        }
        return ranges;
    }
}
