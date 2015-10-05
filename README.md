# awsParallelScan
Simple aws utility for parallel scanning where 
1 or more  threads producer provide record scanning 
and pass in chunk to consumer threads.

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