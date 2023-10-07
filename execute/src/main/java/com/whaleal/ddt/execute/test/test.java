/*
 * Document Data Transfer - An open-source project licensed under GPL+SSPL
 *
 * Copyright (C) [2023 - present ] [Whaleal]
 *
 * This program is free software; you can redistribute it and/or modify it under the terms
 * of the GNU General Public License and Server Side Public License (SSPL) as published by
 * the Free Software Foundation; either version 2 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License and SSPL for more details.
 *
 * For more information, visit the official website: [www.whaleal.com]
 */
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
