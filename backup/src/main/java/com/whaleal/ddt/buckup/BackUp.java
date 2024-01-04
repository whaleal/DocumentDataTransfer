package com.whaleal.ddt.buckup;


import com.alibaba.fastjson2.JSON;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.whaleal.ddt.util.HttpClientPost;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author liheping
 */
@Log4j2
public class BackUp {

    private static MongoClient sourceMongoClient;

    private static MongoClient targetMongoClient;

    public static void main(String[] args) {

        sourceMongoClient = MongoClients.create(args[0]);
        targetMongoClient = MongoClients.create(args[1]);
        Cache<Document> documentCache = new Cache<>(10240);
        MongoNamespace ns = new MongoNamespace(args[2]);
        documentCache.setWapURL(args[4]);

        String workName = "backUpOplog";
        try {
            targetMongoClient.getDatabase(ns.getDatabaseName()).getCollection(ns.getCollectionName()).drop();
            targetMongoClient.getDatabase(ns.getDatabaseName()).getCollection(ns.getCollectionName()).createIndex(new Document().append("ts", 1));
        } catch (Exception e) {

        }

        for (int i = 10; i > 0; i--) {
            new Thread(new Writer(documentCache, ns.getFullName(), targetMongoClient)).start();
        }

        new Thread(new Reader(documentCache, sourceMongoClient, workName, Integer.parseInt(args[3]), Integer.MAX_VALUE)).start();

        long previousTotalWriteNum = Writer.getTotalWriteNum();
        long previousTotalReadNum = Reader.getTotalReadNum();
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // 获取并打印每个参数的速率
            long currentTotalWriteNum = Writer.getTotalWriteNum();
            long currentTotalReadNum = Reader.getTotalReadNum();

            long writeRate = Math.round((currentTotalWriteNum - previousTotalWriteNum) / 10.D);
            long readRate = Math.round((currentTotalReadNum - previousTotalReadNum) / 10.D);

            log.info("totalWriteNum rate:{} per second", writeRate);
            log.info("cache size: {}", documentCache.getQueueOfEvent().size());
            log.info("totalReadNum rate: {} per second", readRate);
            log.info("oplog ts:{}" + documentCache.getOplogTs());

            {
                // 保存配置参数
                Map<String, Object> ddtInfo = new HashMap<>();
                ddtInfo.put("writeRate", writeRate);
                ddtInfo.put("oplogTs", documentCache.getOplogTs());
                ddtInfo.put("totalCount", Writer.getTotalWriteNum());
                // 10s 保存一次
                HttpClientPost.postJson(documentCache.getWapURL(), JSON.toJSONString(ddtInfo));
            }

            // 更新前一个时间和值以便下一次计算速率
            previousTotalWriteNum = currentTotalWriteNum;
            previousTotalReadNum = currentTotalReadNum;
        }
    }
}
