# awsParallelScan
Simple aws utility for parallel scanning where
1 or more threads producer provide records  scanned
and pass them in a chunk to the consumer threads 

```java

AmazonScanSystem<Contact> scanSys=new AmazonScanSystem<>(dbclient);

 scanSys
 .producer("nametable", 10, 4)
 .consumer(10, consumerprocessor)
 .setMapper(mapper)
 .setMonitorKey("contactMonitor")
 .addListenener(scanCallBack)
 .scan();

```

Everything was done very quickly .
Welcome to any criticism and any help .
thank you.