package com.dnocode.utils.aws.model;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAutoGeneratedKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;

@DynamoDBTable(tableName = "contact")
public class Contact {



    String id;
    String firstName;
    String lastName;
    public String nameAndSurname;

    @DynamoDBHashKey
    @DynamoDBAutoGeneratedKey
    public String getId(){return id;}

    @DynamoDBAttribute
    public String getFirstName()
    {
        return firstName;
    }

    @DynamoDBAttribute
    public String getLastName()
    {
        return lastName;
    }

    public void setId(String id) {
        this.id = id;
    }
    public void setLastName(String lastName) {
        this.lastName = lastName;
    }

    public void setFirstName(String firstName) {

        this.firstName = firstName;
    }
}
