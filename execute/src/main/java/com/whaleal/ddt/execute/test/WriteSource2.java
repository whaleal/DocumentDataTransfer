package com.whaleal.ddt.execute.test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.WriteModel;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * @projectName: full-common
 * @package: com.whaleal.ddt.execute.test
 * @className: WriteSource
 * @author: Eric
 * @description: TODO
 * @date: 14/11/2023 18:18
 * @version: 1.0
 */
public class WriteSource2 {
    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create("mongodb://192.168.12.100:26789");

        MongoDatabase database = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("test");

        List<WriteModel<Document>> writeModelList = new ArrayList<>();


        collection.insertOne(new Document("_id", "lhp").append("value", "test"));
        collection.updateOne(new Document("_id", "lhp"), new Document("$set", new Document("value", "update")));

    }
}
