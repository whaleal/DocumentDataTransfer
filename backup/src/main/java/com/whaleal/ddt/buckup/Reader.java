package com.whaleal.ddt.buckup;


import com.mongodb.BasicDBObject;
import com.mongodb.CursorType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import lombok.extern.log4j.Log4j2;
import org.bson.BsonTimestamp;
import org.bson.Document;

@Log4j2
public class Reader implements Runnable {


    private static long totalReadNum;

    public static long getTotalReadNum() {
        return totalReadNum;
    }

    @Override
    public void run() {
        while (!isReadScanOver) {
            source();
        }
    }

    private Cache<Document> documentCache;

    protected boolean isReadScanOver = false;

    private static final Document PROJECT_FIELD = new Document();

    static {
        PROJECT_FIELD.put("ts", 1);
        PROJECT_FIELD.put("ns", 1);
        PROJECT_FIELD.put("o", 1);
        PROJECT_FIELD.put("o2", 1);
        PROJECT_FIELD.put("op", 1);
        // shard迁移时 使用该字段
        PROJECT_FIELD.put("fromMigrate", 1);
    }

    protected BsonTimestamp lastOplogTs = new BsonTimestamp(0);

    MongoClient mongoClient;

    String workName;

    int startTimeOfOplog;

    int endTimeOfOplog;

    public Reader(Cache<Document> documentCache, MongoClient mongoClient, String workName, int startTimeOfOplog, int endTimeOfOplog) {
        this.documentCache = documentCache;
        this.mongoClient = mongoClient;
        this.workName = workName;
        this.startTimeOfOplog = startTimeOfOplog;
        this.endTimeOfOplog = endTimeOfOplog;
    }

    private BasicDBObject generateCondition(BsonTimestamp docTime) {
        BasicDBObject condition = new BasicDBObject();
        if (startTimeOfOplog != 0) {
            // 设置查询数据的时间范围
            condition.append("ts", new Document().append("$gte", docTime));
        }
        if (endTimeOfOplog != 0) {
            // 带有范围的oplog
            condition.append("ts", new Document().append("$gte", docTime).append("$lte", new BsonTimestamp(endTimeOfOplog, 0)));
        }
        log.info("{} the conditions for reading oplog.rs :{}", workName, condition.toJson());
        return condition;
    }

    public void source() {
        BsonTimestamp docTime = new BsonTimestamp(startTimeOfOplog, 0);
        log.info("{} start reading oplog data", workName);
        int readNum = 1024000;
        try {
            MongoCollection oplogCollection = mongoClient.getDatabase("local").getCollection("oplog.rs");
            MongoCursor<Document> cursor =
                    oplogCollection.find(generateCondition(docTime)).projection(PROJECT_FIELD).
                            sort(new Document("$natural", 1)).
                            cursorType(CursorType.TailableAwait).noCursorTimeout(true).batchSize(8096).iterator();

            while (cursor.hasNext()) {
                totalReadNum++;
                Document document = cursor.next();
                // 10w条输出一次 或者10s输出一次
                if (readNum++ > 1024000 || (((BsonTimestamp) document.get("ts")).getTime() - lastOplogTs.getTime() > 60)) {
                    // 记录当前oplog的时间
                    lastOplogTs = (BsonTimestamp) document.get("ts");
                    documentCache.setOplogTs(lastOplogTs.getTime());
                    docTime = (BsonTimestamp) document.get("ts");
                    readNum = 0;
                    log.info("{} current read oplog time:{}", workName, lastOplogTs.getTime());
                    log.info("{} current oplog delay time:{} s", workName, Math.abs(System.currentTimeMillis() / 1000F - lastOplogTs.getTime()));
                }
                // document
                documentCache.getQueueOfEvent().put(document);
            }
            isReadScanOver = true;
        } catch (Exception e) {
            log.info("{} current read oplog time:{}", workName, docTime.getTime());
            isReadScanOver = false;
            // 重新更新查询的开始时间和结束时间
            this.startTimeOfOplog = docTime.getTime();
            log.error("{} read oplog exception,msg:{}", workName, e.getMessage());
        }
    }

}
