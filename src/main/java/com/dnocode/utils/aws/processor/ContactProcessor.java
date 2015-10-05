package com.dnocode.utils.aws.processor;

import com.dnocode.utils.aws.AmazonConsumerThreadPool;
import com.dnocode.utils.aws.model.Contact;
import org.apache.log4j.Logger;

import java.util.List;


/**
 * Created by dnocode on 05/10/15.
 */
public class ContactProcessor extends AmazonConsumerThreadPool.ChunkProcessor {

    final Logger log= Logger.getLogger(ContactProcessor.class);
    private final List<Contact> list;

    public ContactProcessor(List<Contact> contact){

        this.list=contact;
    }

    @Override
    public void processChunk() {

         chunk.forEach(c -> {

             Contact contact = (Contact) c;
             contact.nameAndSurname = contact.getFirstName() + "-" + contact.getLastName();
             list.add(contact);

         });




    }
}
