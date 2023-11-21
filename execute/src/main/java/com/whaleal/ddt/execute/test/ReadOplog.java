package com.whaleal.ddt.execute.test;

import com.mongodb.CursorType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.BsonTimestamp;
import org.bson.Document;

/**
 * @projectName: full-common
 * @package: com.whaleal.ddt.execute.test
 * @className: ReadOplog
 * @author: Eric
 * @description: TODO
 * @date: 16/11/2023 10:13
 * @version: 1.0
 */
public class ReadOplog {
    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create("mongodb://192.168.12.100:26789");

        MongoCollection oplogCollection = mongoClient.getDatabase("local").getCollection("oplog.rs");
        MongoCursor<Document> cursor =
                oplogCollection.find(new Document("ts",new Document("$gte",new BsonTimestamp((int) (System.currentTimeMillis()/1000),0)))).
                        sort(new Document("$natural", 1)).
                        cursorType(CursorType.TailableAwait).noCursorTimeout(true).batchSize(1024).iterator();



        while (cursor.hasNext()) {
            Document document = cursor.next();
            System.out.println(document.toJson());
        }
    }
}
