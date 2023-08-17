package com.whaleal.ddt.execute.test;


import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import org.bson.Document;

public class test {


    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create("mongodb://server100:24578");

        BasicDBObject basicDBObject = new BasicDBObject("ns", "source.lhp6");

        MongoCursor<Document> cursor = mongoClient.getDatabase("local").getCollection("oplog.rs").find(basicDBObject).cursor();
        while (cursor.hasNext()) {
            Document next = cursor.next();
            System.out.println(next.toJson());
        }

        mongoClient.close();
    }

//    public static void targetWriteData(String ns, Document data) {
//        try {
//            MongoClient mongoClient = MongoClients.create("mongodb://admin:123456@192.168.12.100:57018/admin");
//            mongoClient.getDatabase(ns.split("\\.", 2)[0]).getCollection(ns.split("\\.", 2)[1]).insertOne(data);
//            mongoClient.close();
//        } catch (Exception e) {
//        }
//    }

    public static void getUserInfo() {
        MongoClient mongoClient = MongoClients.create("mongodb://root:123456@192.168.11.100:37018/admin");
        System.out.println("====================admin.system.users");
        final MongoCursor<Document> cursor = mongoClient.getDatabase("admin").getCollection("system.users").find(new BasicDBObject()).cursor();
        while (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());
        }
    }
}
