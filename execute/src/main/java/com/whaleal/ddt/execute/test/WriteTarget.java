package com.whaleal.ddt.execute.test;

import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @projectName: full-common
 * @package: com.whaleal.ddt.execute.test
 * @className: WriteSource
 * @author: Eric
 * @description: TODO
 * @date: 14/11/2023 18:18
 * @version: 1.0
 */
public class WriteTarget {
    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create("mongodb://192.168.12.200:26789");
        MongoDatabase database = mongoClient.getDatabase("test");
        MongoCollection<Document> collection = database.getCollection("test");
        List<WriteModel<Document>> writeModelList = new ArrayList<>();
        while (true) {

            for (int i = 0; i < 128; i++) {
                writeModelList.add(new InsertOneModel<>(new Document("value", "target").append("_id", "target-" + Math.round(Math.random() * 1000000))));
                writeModelList.add(new InsertOneModel<>(new Document("value", "target").append("_id", "target-" + Math.round(Math.random() * 1000000))));
                writeModelList.add(new UpdateOneModel<Document>(Filters.eq("_id", "target-" + Math.round(Math.random() * 1000000)),
                        new Document("$set", new Document("value", "target" + System.currentTimeMillis()).append("updateValue", "target"))));
                writeModelList.add(new ReplaceOneModel<>(Filters.eq("_id", "target-" + Math.round(Math.random() * 1000000)),
                        new Document("value", "target-replace").append("replaceValue", "target")));
                writeModelList.add(new DeleteOneModel<>(Filters.eq("_id", "target-" + Math.round(Math.random() * 1000000))));
            }

            try {
                collection.bulkWrite(writeModelList, new BulkWriteOptions().ordered(false));
            } catch (Exception e) {
            } finally {
                writeModelList = new ArrayList<>();
            }
        }
    }
}
