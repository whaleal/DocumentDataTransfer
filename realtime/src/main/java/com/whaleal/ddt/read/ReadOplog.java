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
package com.whaleal.ddt.read;

import com.mongodb.BasicDBObject;
import com.mongodb.CursorType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.whaleal.ddt.cache.MetadataOplog;
import com.whaleal.ddt.connection.MongoDBConnection;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.task.CommonTask;
import lombok.extern.log4j.Log4j2;
import org.bson.BsonTimestamp;
import org.bson.Document;

import java.util.concurrent.TimeUnit;

/**
 * @author: lhp
 * @time: 2021/7/21 2:38 下午
 * @desc: 读取oplog中的数据
 */
@Log4j2
public class ReadOplog extends CommonTask {
    /**
     * project属性
     */
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


    /**
     * mongoClient
     */
    private final MongoClient mongoClient;
    /**
     * 表过滤策略 使用正则表达式进行过滤
     */
    private final String dbTableWhite;
    /**
     * 开始读取该数据源的时间
     */
    private int startTimeOfOplog;
    /**
     * 延迟时间s
     */
    private int delayTime = 0;
    /**
     * 结束读取该oplog的时间
     */
    private final int endTimeOfOplog;
    /**
     * 是否同步DDL
     */
    private final boolean filterDdl;
    /**
     * oplog元数据库类保存数据信息的地方
     */
    private final MetadataOplog metadataOplog;
    /**
     * 是否读取完成
     */
    private boolean isReadScanOver = false;
    /**
     * 数据源版本
     */
    private final String dbVersion;

    /**
     * 最新oplog时间戳信息
     */
    public BsonTimestamp lastOplogTs = new BsonTimestamp(0);

    public ReadOplog(String workName, String dsName, boolean isFilterDdl, String dbTableWhite, int startTimeOfOplog, int endTimeOfOplog, int delayTime) {
        super(workName, dsName);
        this.filterDdl = isFilterDdl;
        this.dbTableWhite = dbTableWhite;
        this.endTimeOfOplog = endTimeOfOplog;
        this.startTimeOfOplog = startTimeOfOplog;
        this.workName = workName;
        this.delayTime = delayTime;
        this.metadataOplog = MetadataOplog.getOplogMetadata(workName);
        this.mongoClient = MongoDBConnection.getMongoClient(dsName);
        this.dbVersion = MongoDBConnection.getVersion(dsName);
    }


    @Override
    public void execute() {
        // 当出现异常时 可以进行进行查询
        while (!isReadScanOver) {
            log.info("{} ready to read oplog", workName);
            try {
                BsonTimestamp startTime = new BsonTimestamp(startTimeOfOplog, 0);
                // 获取oplog的最早时间
                BsonTimestamp oplogStartTime = getStartTimeOfOplog();
                log.info("{} check if sliding window time is missed:{ MongodbT start time:{},oplog.rs earliest record time:{} }", workName, startTime, startTimeOfOplog);
                // 如果oplog的开始时间小于startTimeOfReady，即全表同步期间oplog没有被覆盖
                // startTimeOfReady=0时代表为增量抽取数据。抽取范围[minTs,正无穷)
                // 程序开始时间要大小oplog的开始时间60s,但是此时还不能保证原子性问题,可能出现游标掉线的问题
                if ((startTime.getTime() - oplogStartTime.getTime()) < 60 && startTimeOfOplog != 0) {
                    log.error("{} failed to read oplog: missed sliding window time oplog is overwritten, this program is about to exit", workName);
                    break;
                }
                source();
            } catch (Exception e) {
                log.error("{} error reading oplog failed,msg:{}", workName, e.getMessage());
            }
        }
    }

    /**
     * getStartTimeOfOplog 获取oplog的开始时间
     *
     * @return BsonTimestamp 第一条oplog的数据
     * @desc 获取oplog的第一条数据的时间
     */
    public BsonTimestamp getStartTimeOfOplog() {
        for (int i = 0; i < 3; i++) {
            try {
                MongoCollection topicCollection = mongoClient.getDatabase("local").getCollection("oplog.rs");
                Document document = (Document) topicCollection.find().sort(new Document("$natural", 1)).first();
                BsonTimestamp bsonTimestamp = (BsonTimestamp) document.get("ts");
                if (bsonTimestamp.getTime() > 0) {
                    return bsonTimestamp;
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("{} error getting first record of oplog.rs,msg:{}", workName, e.getMessage());
                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (Exception ignored) {
                }
            }
        }
        // Q： 实在查询不到 ts
        // A: 0
        return new BsonTimestamp(0);
    }


    /**
     * source 读取oplog的中数据
     *
     * @desc 读取oplog的中数据
     */
    public void source() {
        BsonTimestamp docTime = new BsonTimestamp(startTimeOfOplog, 0);
        log.info("{} start reading oplog data", workName);
        BasicDBObject condition = new BasicDBObject();
        if (startTimeOfOplog != 0) {
            // 设置查询数据的时间范围
            condition.append("ts", new Document().append("$gte", docTime));
        }
        if (endTimeOfOplog != 0) {
            // 带有范围的oplog
            condition.append("ts", new Document().append("$gte", docTime).append("$lte", new BsonTimestamp(endTimeOfOplog, 0)));
        }
        //   Q：读取全部数据，会造成带宽浪费
        //   A：增加正则表达式读取ns
        // 不为全部库表
        if (!".+".equals(dbTableWhite)) {
            String regexStr = "(" + dbTableWhite + ")";
//            if (filterDdl) {
                // 读取所有的cmd和所有的system表
                // system 跟视图 时许表 及3.2中的建立索引有关系
                regexStr += "|(.+\\.\\$cmd)|(.+\\.system\\..+)";
//            }
            condition.append("ns", new Document("$regex", regexStr));
        }
        log.info("{} the conditions for reading oplog.rs :{}", workName, condition.toJson());
        int readNum = 0;
        try {
            MongoCollection oplogCollection = mongoClient.getDatabase("local").getCollection("oplog.rs");
            MongoCursor<Document> cursor =
                    oplogCollection.find(condition).projection(PROJECT_FIELD).
                            sort(new Document("$natural", 1)).
                            cursorType(CursorType.TailableAwait).noCursorTimeout(true).batchSize(8192).iterator();
            while (cursor.hasNext()) {
                Document document = cursor.next();
                String ns = document.get("ns").toString();

                // 10w条输出一次 或者10s输出一次
                if (readNum++ > 102400 || (((BsonTimestamp) document.get("ts")).getTime() - lastOplogTs.getTime() > 60)) {
                    // 记录当前oplog的时间
                    lastOplogTs = (BsonTimestamp) document.get("ts");
                    readNum = 0;
                    log.info("{} current read oplog time:{}", workName, lastOplogTs.getTime());
                    log.info("{} 当前oplog延迟时间:{} s", workName, Math.abs(System.currentTimeMillis() / 1000F - lastOplogTs.getTime()));
                    // q: 如果后面一直没有数据的话，这个信息就一直不打印。确实会出现日志不全的问题
                    // a: 为避免主线程的业务侵入性，暂时取舍。若是一直无oplog那就不打印罢了

                    // 只有增量任务才有进度百分比
                    if (endTimeOfOplog != 0) {
                        // endTimeOfOplog- startTimeOfOplog 的总时间
                        // endTimeOfOplog -lastOplogTs 的总时间
                        int percentage = (Math.round((0.0F + lastOplogTs.getTime() - startTimeOfOplog) / ((0.0F + endTimeOfOplog - startTimeOfOplog))));
                        if (percentage < 0) {
                            percentage = 0;
                        }
                        log.info("{} 当前增量进度{}%", workName, percentage);
                    }
                }


                // 判断是否在窗口期范围内
                if (lastOplogTs.getTime() < startTimeOfOplog) {
                    log.error("{} failed to read oplog: missed sliding window time oplog is overwritten, this program is about to exit", workName);
                    WorkStatus.updateWorkStatus(workName, WorkStatus.WORK_STOP);
                }


                {
                    // 检查任务状态
                    if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_STOP) {
                        break;
                    }
                    if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_PAUSE) {
                        // 发生了限速就开始限制读取
                        while (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_PAUSE) {
                            TimeUnit.SECONDS.sleep(5);
                        }
                    }
                }
                //  单独为3.2进行判断建立索引
                if (this.dbVersion.startsWith("3") && ns.endsWith(".system.indexes")) {
                    // 可能为建立索引
                    Document o = (Document) document.get("o");
                    // 插入数据 但是没有_id 。则是建立索引的oplog
                    if (!o.containsKey("_id")) {
                        // 开始构造通用的建立索引的oplog
                        document.put("op", "c");
                        String dbName = ns.split("\\.", 2)[0];
                        document.put("ns", dbName + ".$cmd");
                        String tableName = o.get("ns").toString().split("\\.", 2)[1];
                        o.put("createIndexes", tableName);
                        o.remove("ns");
                        document.put("o", o);
                        ns = dbName + ".$cmd";
                    }
                }
                // 库表名过滤.
                if (!ns.matches(dbTableWhite)) {
                    // 没有通过库表过滤,则进入判断是否通过ddl判断
                    // cmd的进行下一级步骤进行过滤
                    if (!filterDdl || !"c".equals(document.get("op"))) {
                        continue;
                    }
                }

                // 过滤元数据库信息 local admin config库数据不同步
                if (!ns.startsWith("local.") && !ns.startsWith("admin.") && !ns.startsWith("config.")) {
                    // 保留本次oplog的ts读取时间
                    // 当前时间减去oplog的时间减去  小于延迟时间即可
                    if (delayTime > 0 && (System.currentTimeMillis() / 1000) - docTime.getTime() < delayTime) {
                        // 先把超时的时间进行睡眠等待
                        TimeUnit.SECONDS.sleep((System.currentTimeMillis() / 1000) - docTime.getTime());
                        TimeUnit.MINUTES.sleep(1);
                    }
                    // 保留本次oplog的ts读取时间
                    metadataOplog.getQueueOfOplog().put(document);
                    metadataOplog.getReadNum().add(1);
                    docTime = (BsonTimestamp) document.get("ts");
                }
            }
            // 如果程序能够正常走到这里 则代表查询完毕 更新程序的状态
            WorkStatus.updateWorkStatus(workName, WorkStatus.WORK_STOP);
            isReadScanOver = true;
        } catch (Exception e) {
            log.info("{} current read oplog time:{}", workName, docTime.getTime());
            isReadScanOver = false;
            // 重新更新查询的开始时间和结束时间
            this.startTimeOfOplog = docTime.getTime();
            log.error("{} read oplog exception,msg:{}", workName, e.getMessage());
        }
    }

    public static void main(String[] args) {
        final BsonTimestamp bsonTimestamp = new BsonTimestamp(System.currentTimeMillis());
        System.out.println(bsonTimestamp.getValue());
        System.out.println(bsonTimestamp.getTime());


        Document condition = new Document();
//        condition.append("ts", new Document().append("$gte", 0).append("$lte", new BsonTimestamp(1691486720, 0)));


        condition.append("ns", new Document("$regex", "(lhp100.+)|(.+\\.\\$cmd)|(.+\\.system\\..+)"));
        System.out.println(condition.toJson());
    }
}
