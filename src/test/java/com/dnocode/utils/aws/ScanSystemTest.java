package com.dnocode.utils.aws;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.model.*;
import com.dnocode.utils.aws.model.Contact;
import com.dnocode.utils.aws.processor.ContactProcessor;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import static org.junit.Assert.assertTrue;

public class ScanSystemTest {

    public static  AmazonDynamoDBClient dbclient;
    final static Logger log= Logger.getLogger(ScanSystemTest.class);
    public List<Contact> result=new ArrayList<>();
    @BeforeClass
    public static void  createTable() throws InterruptedException {

        AWSCredentials credentials = new BasicAWSCredentials(
                "YourAccessKeyID",
                "YourSecretAccessKey");
         dbclient=new AmazonDynamoDBClient(credentials);
        dbclient.setEndpoint("http://localhost:8000");
        DynamoDB dynamoDB = new DynamoDB(dbclient);
        ArrayList<AttributeDefinition> attributeDefinitions= new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName("id").withAttributeType("S"));
        ArrayList<KeySchemaElement> keySchema = new ArrayList();
        keySchema.add(new KeySchemaElement().withAttributeName("id").withKeyType(KeyType.HASH));

        CreateTableRequest request = new CreateTableRequest()
                .withTableName("contact")
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput()
                        .withReadCapacityUnits(1L)
                        .withWriteCapacityUnits(1L));
        Table table = dynamoDB.createTable(request);
        table.waitForActive();
        DynamoDBMapper mapper = new DynamoDBMapper(dbclient);
        IntStream.range(0, 200).forEach( i->

                {
                        Contact contact = new Contact();
                        contact.setFirstName("name-"+i);
                        contact.setLastName("lastname-" + i);
                        mapper.save(contact);

                }
        );



    }

    @Test
    public void dbIsUpTest() {

        assertTrue("contacts are in dynamo local",   dbclient.scan(new ScanRequest("contact")).getCount().intValue()   == 200);
    }


    @Test
    public void scanTest(){

        /**used for process each chunk passed**/
        ContactProcessor contactProcessor=new ContactProcessor(result);


        AmazonScanSystem<Contact> scanSys=new AmazonScanSystem<>(dbclient);

        /**call back calls when all records had been scanned and on each segment scan**/
        ParallelScanThreadPool.ParallelScanListener scanCallBack=new ParallelScanThreadPool.ParallelScanListener() {
            @Override
            public void onSegmentScanTerminated(int itemsScanned, int segmentNumber) {


                log.info("scanned finished items:"+itemsScanned + " segment number :"+segmentNumber);

            }

            @Override
            public void onAllSegmentsScanTerminated(String discriminator) {

                log.info("scan system terminated "+discriminator);

                assertTrue("all contacts have been  retrieved", result.size() == 200);
                assertTrue("contacts are processed correctly", result.get(0).nameAndSurname!=null);

            }
        };


        /**
         * mapper simply interface for mapping amazon item on our object
         */
        ParallelScanThreadPool.MapperClass<Contact> mapper = (item, threadKey) -> {

          Contact contact=new Contact();
          item.entrySet()
          .forEach(r->
                {
                    final String attributeName=r.getKey();

                    switch (attributeName){
                        case "firstName":
                            contact.setFirstName(r.getValue().getS());
                            break;
                        case "lastName":
                            contact.setLastName(r.getValue().getS());
                            break;
                    }

                });

            return contact;
        };



        scanSys
       .producer("contact", 10, 4)
       .consumer(10, contactProcessor)
       .setMapper(mapper)
       .setMonitorKey("contactMonitor")
       .addListenener(scanCallBack)
       .scan();


    }

}
