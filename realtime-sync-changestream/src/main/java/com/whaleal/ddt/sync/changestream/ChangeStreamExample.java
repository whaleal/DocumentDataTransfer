package com.whaleal.ddt.sync.changestream;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;

import static java.util.Collections.singletonList;

public class ChangeStreamExample {
    public static void main(String[] args) {

        MongoClient mongoClient = MongoClients.create("mongodb://192.168.12.200:24578");
        //todo 是否可以条件筛选,fullDocument相关参数,
        //1. 操作的类型如何筛选，筛选namespace，时间问题确认一下



        ChangeStreamIterable<Document> changeStream = mongoClient.watch();
        // 创建ChangeStream
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor()) {
            while (cursor.hasNext()) {
                ChangeStreamDocument<Document> changeEvent = cursor.next();

                System.out.println(changeEvent.getOperationType().getValue());

            }
        } catch (Exception e) {
            // 处理失效事件
            e.printStackTrace();
            System.out.println("ChangeStream失效：" + e.getMessage());
        }
        // 关闭连接
        mongoClient.close();
    }
}
