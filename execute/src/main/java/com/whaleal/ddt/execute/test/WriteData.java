package com.whaleal.ddt.execute.test;

import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

import lombok.extern.log4j.Log4j2;
import org.bson.*;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
@Log4j2
public class WriteData {

    final static Map<String, AtomicLong> BUCK_INFO_MAP = new ConcurrentHashMap<>();

    static {
        BUCK_INFO_MAP.put("i", new AtomicLong());
        BUCK_INFO_MAP.put("u", new AtomicLong());
        BUCK_INFO_MAP.put("d", new AtomicLong());
        BUCK_INFO_MAP.put("c", new AtomicLong());
    }


    public static void main(String[] args) {
//        final String url = args[0];
//        final String dbName = args[1];

        final String url = "mongodb://192.168.12.200:24578";
        final String dbName = "test_lhp"+System.currentTimeMillis();

        new Thread(new Runnable() {
            @Override
            public void run() {
                ddl(url, dbName);
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                insertData(url, dbName);
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                deleteData(url, dbName);
            }
        }).start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                updateData(url, dbName);
            }
        }).start();

        while (true) {
            log.info(BUCK_INFO_MAP.toString());
            try {
                TimeUnit.SECONDS.sleep(60);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void ddl(String url, String dbName) {
        MongoClient mongoClient = MongoClients.create(url);
        final MongoDatabase database = mongoClient.getDatabase(dbName);
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(600);
                // 删表
                {
                    String tableName = "test_" + ((int) (Math.random() * 100));
                    try {
                        database.getCollection(tableName).drop();
                        BUCK_INFO_MAP.get("c").addAndGet(1);
                    } catch (Exception ignored) {

                    }
                }
                // 建索引
                {
                    String tableName = "test_" + ((int) (Math.random() * 100));
                    try {
                        database.getCollection(tableName).createIndex(new Document("int", 1));
                        BUCK_INFO_MAP.get("c").addAndGet(1);
                    } catch (Exception ignored) {

                    }

                }
                // 删索引
                {
                    String tableName = "test_" + ((int) (Math.random() * 100));
                    try {
                        database.getCollection(tableName).dropIndex(new Document("int", 1));
                        BUCK_INFO_MAP.get("c").addAndGet(1);
                    } catch (Exception ignored) {

                    }
                }
                // 建唯一索引
                {
                    String tableName = "test_" + ((int) (Math.random() * 100));
                    try {
                        database.getCollection(tableName).createIndex(new Document("int", 1), new IndexOptions().unique(true));
                        BUCK_INFO_MAP.get("c").addAndGet(1);
                    } catch (Exception ignored) {

                    }
                }
                // 删唯一索引
                {
                    String tableName = "test_" + ((int) (Math.random() * 100));
                    try {
                        database.getCollection(tableName).dropIndex(new Document("int", 1));
                        BUCK_INFO_MAP.get("c").addAndGet(1);
                    } catch (Exception ignored) {

                    }
                }
                // 重新命名
                {
                    try {
                        database.getCollection("test_" + ((int) (Math.random() * 100))).renameCollection(new MongoNamespace(dbName, "test_" + ((int) (Math.random() * 100))), new RenameCollectionOptions().dropTarget(true));
                        BUCK_INFO_MAP.get("c").addAndGet(1);
                    } catch (Exception ignored) {

                    }
                    try {
                        database.getCollection("test_" + ((int) (Math.random() * 100))).renameCollection(new MongoNamespace(dbName, "test_" + ((int) (Math.random() * 100))), new RenameCollectionOptions().dropTarget(true));
                        BUCK_INFO_MAP.get("c").addAndGet(1);
                    } catch (Exception ignored) {

                    }
                    try {
                        database.getCollection("test_" + ((int) (Math.random() * 100))).renameCollection(new MongoNamespace(dbName, "test_" + ((int) (Math.random() * 100))), new RenameCollectionOptions().dropTarget(true));
                        BUCK_INFO_MAP.get("c").addAndGet(1);
                    } catch (Exception ignored) {

                    }
                }

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public static void updateData(String url, String dbName) {
        MongoClient mongoClient = MongoClients.create(url);
        final MongoDatabase database = mongoClient.getDatabase(dbName);
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(1);
                final int v = (int) (Math.random() * 100000);
                for (long i = v - 1000; i < v; i++) {
                    String tableName = "test_" + ((int) (Math.random() * 100));
                    final UpdateResult updateResult = database.getCollection(tableName).updateMany(new Document("int", i), new Document("$set", new Document("newDate", new Date()).append("newInt", i)));
                    BUCK_INFO_MAP.get("u").addAndGet(updateResult.getModifiedCount());
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }


    public static void deleteData(String url, String dbName) {
        MongoClient mongoClient = MongoClients.create(url);
        final MongoDatabase database = mongoClient.getDatabase(dbName);
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(1);
                final int v = (int) (Math.random() * 100000);
                for (long i = v - 1000; i < v; i++) {
                    String tableName = "test_" + ((int) (Math.random() * 100));
                    final DeleteResult deleteResult = database.getCollection(tableName).deleteMany(new Document("int", i));
                    BUCK_INFO_MAP.get("d").addAndGet(deleteResult.getDeletedCount());
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public static void insertData(String url, String dbName) {
        MongoClient mongoClient = MongoClients.create(url);
        final MongoDatabase database = mongoClient.getDatabase(dbName);
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(2);
                List<WriteModel<Document>> insertList = new ArrayList<>();
                final int v = (int) (Math.random() * 100000);
                for (long i = v - 1000; i < v; i++) {
                    Document insert = new Document();
                    insert.append("int", i);
                    insert.append("int2", i);
                    insert.append("BsonTimestamp1", new BsonTimestamp((int) (System.currentTimeMillis() / 1000), 78));
                    insert.append("String", "strlhp李和平asdfgdsf");
                    insert.append("Doc", new Document().append("1waedfsg", 1));
                    insert.append("javaInt", i);
                    insert.append("bytes", new byte[]{1});
                    insert.append("Array", new ArrayList<String>());
                    insert.append("Binary data", new Binary(new byte[]{1, 2, 3}));
                    insert.append("ObjectId", new ObjectId());
                    insert.append("Boolean", false);
                    insert.append("Date", new Date());
                    insert.append("Null", null);
                    insert.append("Regular Expression", new BsonRegularExpression("lhp.*"));
                    insert.append("DBPointer", new BsonDbPointer("1", new ObjectId()));
                    insert.append("Undefined", new BsonUndefined());
                    insert.append("JavaScript", new BsonJavaScript("var i=0"));
                    insert.append("Symbol", new BsonSymbol("var i=0"));
                    insert.append("BsonStr", new BsonString("var i=0"));
                    insert.append("BsonJavaScriptWithScope", new BsonJavaScriptWithScope("var i=0", new BsonDocument()));
                    insert.append("32integer", new BsonInt32(12));
                    insert.append("64int", new BsonInt64(123));
                    insert.append("Min key", new BsonMinKey());
                    insert.append("Max key", new BsonMaxKey());
                    insert.append("BsonTimestamp", new BsonTimestamp());
                    insertList.add(new InsertOneModel<>(insert));
                }
                final BulkWriteResult bulkWriteResult = database.getCollection("test_" + ((int) (Math.random() * 100))).bulkWrite(insertList, new BulkWriteOptions().ordered(false));
                BUCK_INFO_MAP.get("i").addAndGet(bulkWriteResult.getInsertedCount());
                final BulkWriteResult bulkWriteResult2 = database.getCollection("test_" + System.currentTimeMillis() / 1000 / 1000).bulkWrite(insertList, new BulkWriteOptions().ordered(false));
                BUCK_INFO_MAP.get("i").addAndGet(bulkWriteResult2.getInsertedCount());
            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }
}
