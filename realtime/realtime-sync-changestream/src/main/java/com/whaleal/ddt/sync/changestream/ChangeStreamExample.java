package com.whaleal.ddt.sync.changestream;

import com.alibaba.fastjson2.JSON;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

public class ChangeStreamExample {
    public static void main(String[] args) {

        MongoClient mongoClient = MongoClients.create("mongodb://192.168.12.100:27600");
        //todo 是否可以条件筛选,fullDocument相关参数,
        //1. 操作的类型如何筛选，筛选namespace，时间问题确认一下

        List<Bson> pipeline = new ArrayList<>();


        pipeline.add(Aggregates.match(Filters.and(new Document("clusterTime", new Document().append("$gte", new BsonTimestamp((int) (System.currentTimeMillis()/1000), 0))))));

//        pipeline.add(new Document("$addFields", new Document("nsToString", new Document("$toString", "$ns"))));

//        pipeline.add(new Document("$match", new Document("nsToString", "doc.lhp")));

        ChangeStreamIterable<Document> changeStream = mongoClient.watch(pipeline);


//        changeStream.startAtOperationTime();

        // 创建ChangeStream
        try (MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor()) {
            while (cursor.hasNext()) {
                ChangeStreamDocument<Document> changeEvent = cursor.next();

                System.out.println(changeEvent.getOperationType().getValue());

                if (changeEvent.getOperationType().getValue().equals("update")) {
                    System.out.println(changeEvent.getUpdateDescription());
                    System.out.println(changeEvent.getUpdateDescription().getTruncatedArrays());
                }


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
