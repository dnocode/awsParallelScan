package com.dnocode.utils.aws;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.util.Map;
import java.util.concurrent.LinkedTransferQueue;

public class AmazonScanSystem<T> {

    private  String scannerTableName;
    private  int scannerPageSize;
    private int scannerThreads;
    private AmazonDynamoDBClient dynamoDBClient;
    private  AmazonConsumerThreadPool consumerThreadPool;

    private  ParallelScanThreadPool producerScannerThread;
    private final LinkedTransferQueue <T>sharedTranferList=new LinkedTransferQueue();
    public AmazonScanSystem(AmazonDynamoDBClient dbclient){  this.dynamoDBClient=dbclient;}

    public ConsumerMaker producer(String tableName,int pageSize,int threadsNumber){
        this.scannerTableName=tableName;
        this.scannerPageSize=pageSize;
        this.scannerThreads=threadsNumber;
        return new ConsumerMaker();
    }

    public class ConsumerMaker{

           public   ScannerParametrizer consumer(int chunkSize,AmazonConsumerThreadPool.ChunkProcessor ... chunkProcessors){

            consumerThreadPool=new AmazonConsumerThreadPool(sharedTranferList,chunkSize,chunkProcessors);
            producerScannerThread=new ParallelScanThreadPool(scannerTableName,scannerPageSize,scannerThreads,sharedTranferList,dynamoDBClient);
            producerScannerThread.stopDependsOn(consumerThreadPool.getThreadPool());
            return new ScannerParametrizer();
        }

         public class ScannerParametrizer {

             String scanfilter;
             String projectExpression;
             Map<String, AttributeValue> values;
             Map<String, String> names;
             String monitorkey;

             ParallelScanThreadPool.MapperClass mapper;

             public ScannerParametrizer() { }
             public ScannerParametrizer setScanFilter(String scanFilter){this.scanfilter=scanFilter; return this;}
             public ScannerParametrizer setProjectExpression(String projectExpression){this.projectExpression=projectExpression;  return this;}
             public ScannerParametrizer setNamesAndValues(Map<String, String> names,Map<String, AttributeValue> values){this.names=names;this.values=values;  return this;}
             public ScannerParametrizer setMapper(ParallelScanThreadPool.MapperClass mapper){ this.mapper=mapper;  return this;}
             public ScannerParametrizer setMonitorKey(String monitorKey) { this.monitorkey=monitorKey;  return this;}
             public ScannerParametrizer addListenener(ParallelScanThreadPool.ParallelScanListener listener){ producerScannerThread.addListener(listener);  return this; }

             public void scan(){
                 consumerThreadPool.listen();
                 producerScannerThread.parallelScan(scanfilter,projectExpression,values,names,mapper,monitorkey.toString()); }
         }
    }

}
