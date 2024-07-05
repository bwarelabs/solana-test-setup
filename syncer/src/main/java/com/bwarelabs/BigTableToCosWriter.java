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
    private static final int THREAD_COUNT = 16;
    private static final int SUBRANGE_SIZE = 4; // Number of rows to process in each batch within a thread range
    private static final int BATCH_LIMIT = 5;  // Limit the number of chained batches
    private final List<CompletableFuture<Void>> allUploadFutures = new ArrayList<>();
    private final Map<Integer, String> checkpoints = new HashMap<>();

    private static final char[] CHARACTERS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz".toCharArray();

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

        if (hexRanges.size() != THREAD_COUNT || txRanges.size() != THREAD_COUNT) {
            logger.severe("Invalid number of thread ranges, size must be equal to THREAD_COUNT");
            return;
        }

        // todo - add the rest of the tables
        List<String> tables = List.of("tx");
        List<String> startingKeysForTx = new ArrayList<>();

        for (int i = 0; i < THREAD_COUNT; i++) {
            String[] txRange = txRanges.get(i);
            String txStartKey = getThreadStartingKey("tx", txRange[0], txRange[1]);
            startingKeysForTx.add(txStartKey);

            logger.info(String.format("Range: %s - %s", txRange[0], txRange[1]));
            logger.info("Starting key for thread " + i + " is " + txStartKey);
        }


        for (int i = 0; i < THREAD_COUNT; i++) {
//            String[] hexRange = hexRanges.get(i);
            String[] txRange = txRanges.get(i);

            logger.info(String.format("Range: %s - %s", txRange[0], txRange[1]));

            logger.info("Getting starting key for thread " + i);
            String txStartKey = getThreadStartingKey("tx", txRange[0], txRange[1]);
            startingKeysForTx.add(txStartKey);
            logger.info("Starting key for thread " + i + " is " + txStartKey);

            for (String table : tables) {
                String startRow = checkpoints.getOrDefault(i, table.equals("tx") ? startingKeysForTx.get(i) : hexRanges.get(i)[0]);
                String endRow = hexRanges.get(i)[1];

                boolean isCheckpointStart = table.equals("tx") && checkpoints.get(i) != null;

                if (table.equals("tx")) {
                    if (i == THREAD_COUNT - 1) {
                        // set the last existing key in the table as the end row
                        endRow = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
                    } else {
                        endRow = startingKeysForTx.get(i + 1);
                    }
                }

                logger.info(String.format("Table: %s, Range: %s - %s", table, startRow, endRow));
                processTableRangeTx(i, table, startRow, endRow, isCheckpointStart);
            }

//            for (String table : tables) {
//                String startRow = checkpoints.getOrDefault(i, table.equals("tx") || table.equals("tx-by-addr") ? txRange[0] : hexRange[0]);
//                String endRow = table.equals("tx") || table.equals("tx-by-addr") ? txRange[1] : hexRange[1];
//
//                logger.info(String.format("Table: %s, Range: %s - %s", table, startRow, endRow));
////                processTableRange(i, table, startRow, endRow);
//            }
        }

        CompletableFuture<Void> allUploads = CompletableFuture.allOf(allUploadFutures.toArray(new CompletableFuture[0]));
        allUploads.join();

        executorService.shutdown();
        logger.info("All tables processed and uploaded.");
    }

    private String getThreadStartingKey(String tableName, String prefix, String maxPrefix) {
        String result = null;

        try {
            Table table = connection.getTable(TableName.valueOf(tableName));

            // in case there is no key with the prefix, we will try to get the next key
            // todo! - make sure there is one char in the prefix
            int prefixValue = prefix.charAt(0);
            int maxPrefixValue = maxPrefix.charAt(0);
            for (int i = prefixValue; i <= maxPrefixValue; i++) {
//                scan by prefix until we find a key using setStartStopRowForPrefixScan
                Scan scan = new Scan();
                scan.setStartStopRowForPrefixScan(Bytes.toBytes(String.valueOf((char) i)));
                scan.setLimit(1);
                ResultScanner scanner = table.getScanner(scan);
                Result next = scanner.next();
                if (next != null) {
                    result = Bytes.toString(next.getRow());
                    break;
                }
            }

            table.close();
            return result;

        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error getting starting key for thread", e);
        }

        return null;
    }

    private void processTableRangeTx(int threadId, String tableName, String startRowKey, String endRowKey, boolean isCheckpointStart){
        CompletableFuture<Void> processingFuture = CompletableFuture.runAsync(() -> {
            try {
                String currentStartRow = startRowKey;
                List<CompletableFuture<Void>> batchFutures = new ArrayList<>();
                boolean includeStartRow = !isCheckpointStart;

                while (currentStartRow.compareTo(endRowKey) <= 0) {
                    for (int i = 0; i < BATCH_LIMIT && currentStartRow.compareTo(endRowKey) <= 0; i++) {

                        List<Result> batch = fetchBatchTx(tableName, currentStartRow, endRowKey, includeStartRow);
                        if (batch.isEmpty()) {
                            logger.info(String.format("[%s] batch size: %s for startRow: %s and endRow: %s", Thread.currentThread().getName(), batch.size(), currentStartRow, endRowKey));
                            // the thread has reached the end of his range
                            currentStartRow =  endRowKey + "Z";
                            break;
                        }

                        // since currentStartRow was not included in the scan, the real startRow is the first row in the batch
                        currentStartRow = Bytes.toString(batch.get(0).getRow());
                        logger.info(String.format("[%s] batch size: %s for startRow: %s and endRow: %s", Thread.currentThread().getName(), batch.size(), currentStartRow, endRowKey));

                        String currentEndRow = Bytes.toString(batch.get(batch.size() - 1).getRow());

                        CompletableFuture<Void> uploadFuture = null;
                        try {
                            CustomS3FSDataOutputStream customFSDataOutputStream = convertToSeqAndStartUpload(tableName, currentStartRow, currentEndRow, batch);

                            logger.info(String.format("[%s] Processing batch %s - %s", Thread.currentThread().getName(), currentStartRow, currentEndRow));

                            uploadFuture = customFSDataOutputStream.getUploadFuture()
                                    .thenRun(() -> {
                                        logger.info(String.format("[%s] Successfully uploaded %s to COS", Thread.currentThread().getName(), customFSDataOutputStream.getS3Key()));
                                    });
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "Error converting batch to sequencefile format", e);
                            return;
                        }

                        batchFutures.add(uploadFuture);

                        currentStartRow = currentEndRow;
                        includeStartRow = false;
                    }

                    // Wait for the current batch of futures to complete
                    CompletableFuture<Void> currentBatch = CompletableFuture.allOf(batchFutures.toArray(new CompletableFuture[0]));
                    currentBatch.join();

                    updateCheckpoint(threadId, currentStartRow);

                    batchFutures.clear();
                }
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error processing table range for " + tableName, e.getMessage());
            }
        }, executorService);

        allUploadFutures.add(processingFuture);
    }


    private void processTableRange(int threadId, String tableName, String startRowKey, String endRowKey) {
        CompletableFuture<Void> processingFuture = CompletableFuture.runAsync(() -> {
            try {
                String currentStartRow = startRowKey;
                List<CompletableFuture<Void>> batchFutures = new ArrayList<>();

                while (currentStartRow.compareTo(endRowKey) <= 0) {
                    for (int i = 0; i < BATCH_LIMIT && currentStartRow.compareTo(endRowKey) <= 0; i++) {
                        String currentEndRow = calculateEndRow(currentStartRow, endRowKey);
                        List<Result> batch = fetchBatch(tableName, currentStartRow, currentEndRow);
                        logger.info(String.format("[%s] bath size: %s for startRow: %s and endRow: %s", Thread.currentThread().getName(), batch.size(), currentStartRow, currentEndRow));

                        CompletableFuture<Void> uploadFuture = null;
                        try {
                            CustomS3FSDataOutputStream customFSDataOutputStream = convertToSeqAndStartUpload(tableName, currentStartRow, currentEndRow, batch);

                            logger.info(String.format("[%s] Processing batch %s - %s", Thread.currentThread().getName(), currentStartRow, currentEndRow));

                            uploadFuture = customFSDataOutputStream.getUploadFuture()
                                    .thenRun(() -> {
                                        logger.info(String.format("[%s] Successfully uploaded %s to COS", Thread.currentThread().getName(), customFSDataOutputStream.getS3Key()));
                                    });
                        } catch (Exception e) {
                            logger.log(Level.SEVERE, "Error converting batch to sequencefile format", e);
                            return;
                        }

                        batchFutures.add(uploadFuture);

                        currentStartRow = incrementRowKey(currentEndRow);
                    }

                    // Wait for the current batch of futures to complete
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

    private List<Result> fetchBatch(String tableName, String startRowKey, String endRowKey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Result> batch = new ArrayList<>();

        if (startRowKey.equals(endRowKey)) {
            logger.info(String.format("[%s] Fetching single row %s", Thread.currentThread().getName(), startRowKey));
            Get get = new Get(Bytes.toBytes(startRowKey));
            Result result = table.get(get);
            if (!result.isEmpty()) {
                batch.add(result);
            }
        } else {
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(startRowKey));
            scan.withStopRow(Bytes.toBytes(endRowKey));
            scan.setCaching(SUBRANGE_SIZE);

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                batch.add(result);
            }
            scanner.close();
        }

        table.close();
        return batch;
    }

    private List<Result> fetchBatchTx(String tableName, String startRowKey, String endRowKey, boolean includeStartRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        List<Result> batch = new ArrayList<>();

        if (startRowKey.equals(endRowKey)) {
            logger.info(String.format("[%s] Fetching single row %s", Thread.currentThread().getName(), startRowKey));
            Get get = new Get(Bytes.toBytes(startRowKey));
            Result result = table.get(get);
            if (!result.isEmpty()) {
                batch.add(result);
            }
        } else {
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(startRowKey), includeStartRow);
            scan.withStopRow(Bytes.toBytes(endRowKey), false);
            scan.setCaching(SUBRANGE_SIZE);
            scan.setLimit(SUBRANGE_SIZE);

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                batch.add(result);
            }
            scanner.close();
        }

        table.close();
        return batch;
    }

    private CustomS3FSDataOutputStream convertToSeqAndStartUpload (String tableName, String startRowKey, String endRowKey, List<Result> batch) throws IOException {
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

        // starts the writing process, saves a future onto customFSDataOutputStream
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


    public static List<String[]> splitRangeTx() {
        List<String[]> intervals = new ArrayList<>();
        int totalChars = CHARACTERS.length;
        int baseIntervalSize = totalChars / THREAD_COUNT;
        int remainingChars = totalChars % THREAD_COUNT;

        int currentIndex = 0;
        for (int i = 0; i < THREAD_COUNT; i++) {
            int intervalSize = baseIntervalSize + (i < remainingChars ? 1 : 0);
            int endIndex = currentIndex + intervalSize - 1;

            String startKey = String.valueOf(CHARACTERS[currentIndex]);
            String endKey = String.valueOf(CHARACTERS[endIndex]);

            intervals.add(new String[]{startKey, endKey});
            currentIndex = endIndex + 1;
        }

        return intervals;
    }
}
