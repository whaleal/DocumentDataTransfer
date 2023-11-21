package com.whaleal.ddt.execute.test;

import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
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
public class ReadChangeStream {
    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create("mongodb://192.168.12.100:26789");

        // 过滤条件 没有加上
        ChangeStreamIterable<Document> changeStream = null;

        changeStream = mongoClient.watch();


        changeStream.showExpandedEvents(true);
        changeStream.fullDocument(FullDocument.UPDATE_LOOKUP);

        // 可以改变这个值 建议可以计算得出
        changeStream.batchSize(1024);
        // 设置开始时间
        changeStream.startAtOperationTime(new BsonTimestamp((int) (System.currentTimeMillis()/1000),0));
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor();

        while (cursor.hasNext()) {
            ChangeStreamDocument<Document> changeEvent = cursor.next();
            System.out.println(changeEvent.toString());
        }
    }
}
