package com.dnocode.utils.aws;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;


public class ParallelScanThreadPool<T> {

    private MapperClass<T> mapper;
    private Map<String, String> names;
    private Map<String, AttributeValue> values;
    private String scanfilter;
    private String projectExpression;
    private AmazonDynamoDBClient dynamoDBClient;
    private String tableName;
    private int itemLimit;
    private int numberOfThreads;
    private final LinkedTransferQueue<T> transferQueue;
    private ExecutorService consumerThreadPool;
    private final List<ParallelScanListener> listeners = new CopyOnWriteArrayList();
    private String discriminator;
    /**
     * time await before shutdown this threadpool
     **/
    private int milliLimit = 0;

    public ParallelScanThreadPool(String tableName, int itemLimit, int numberOfThreads, LinkedTransferQueue transferQueue, AmazonDynamoDBClient dynamoClient) {
        this.tableName = tableName;
        this.itemLimit = itemLimit;
        this.numberOfThreads = numberOfThreads;
        this.transferQueue = transferQueue;
        this.dynamoDBClient = dynamoClient;
    }

    public void parallelScan(String scanFilter, String projectExpression, Map<String, AttributeValue> values, Map<String, String> names, MapperClass<T> mapper, String discriminator) {
        this.values = values;
        this.names = names;
        this.scanfilter = scanFilter;
        this.projectExpression = projectExpression;
        this.mapper = mapper;
        this.discriminator = discriminator;
        //  log.info("Scanning " + tableName + " using " + numberOfThreads + " threads " + itemLimit + " items at a time");

        ExecutorService scannerThreadPool = Executors.newFixedThreadPool(numberOfThreads);
        // Divide DynamoDB table into logical segments
        // Create one task for scanning each segment
        // Each thread will be scanning one segment
        int totalSegments = numberOfThreads;
        for (int segment = 0; segment < totalSegments; segment++) {
            // Runnable task that will only scan one segment
            ScanSegmentRunnable task = new ScanSegmentRunnable(segment);
            // Execute the task
            scannerThreadPool.execute(task);
        }
        shutDownExecutorService(scannerThreadPool);
    }

    //todo limit scan threads after n milliseconds
    public void setAlivelimit(int milliseconds) {
        this.milliLimit = milliseconds;
    }

    public void stopDependsOn(ExecutorService threadPool) {
        this.consumerThreadPool = threadPool;
    }

    protected void shutDownExecutorService(ExecutorService executor) {

        executor.shutdown();
        //until executor is shutdown
        while (!executor.isTerminated()) {
        }
        //until all consumers have finished
        while (consumerThreadPool != null && !consumerThreadPool.isTerminated()) {
        }
        listeners.forEach(l -> l.onAllSegmentsScanTerminated(tableName + "_" + discriminator));
    }

    public void addListener(ParallelScanListener listener) {
        listeners.add(listener);
    }


    public interface MapperClass<T> {
        T mapInstructions(Map<String, AttributeValue> item, String threadKey);
    }

    // Runnable task for scanning a single segment of a DynamoDB table
    private class ScanSegmentRunnable implements Runnable {

        private int segment;

        public ScanSegmentRunnable(int segment) {
            this.segment = segment;
        }

        @Override
        public void run() {

            int totalScannedItemCount = 0;
            ScanResult result = null;

            try {

                do {
                    ScanRequest scanRequest = new ScanRequest()
                            .withTableName(tableName)
                            .withProjectionExpression(projectExpression)
                            .withFilterExpression(scanfilter)
                            .withLimit(itemLimit)
                            .withTotalSegments(numberOfThreads)
                            .withSegment(segment)
                            .withExpressionAttributeNames(names)
                            .withExpressionAttributeValues(values);

                     if (result != null && result.getLastEvaluatedKey() != null) {  scanRequest.withExclusiveStartKey(result.getLastEvaluatedKey());}

                    result = dynamoDBClient.scan(scanRequest);

                    for (Map<String, AttributeValue> item : result.getItems()) {

                        totalScannedItemCount++;
                        /**amazon item to our object**/
                        T element = mapper.mapInstructions(item, segment + "");
                        /**transfer to consumer**/
                        transferQueue.transfer(element);
                    }

                } while (result != null && result.getLastEvaluatedKey() != null);

            } catch (Exception e) {e.printStackTrace();} finally {

                final int finalTotalScannedItemCount = totalScannedItemCount;
                listeners.forEach(l -> l.onSegmentScanTerminated(finalTotalScannedItemCount, segment)
                );


            }
        }
    }

    public interface ParallelScanListener {
        void onSegmentScanTerminated(int itemsScanned, int segmentNumber);
        void onAllSegmentsScanTerminated(String discriminator);
    }
}
