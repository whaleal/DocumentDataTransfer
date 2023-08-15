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
package com.whaleal.ddt.sync.changestream.read;

import com.mongodb.BasicDBObject;
import com.mongodb.client.ChangeStreamIterable;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.sync.changestream.cache.MetadataEvent;
import com.whaleal.ddt.sync.connection.MongoDBConnectionSync;
import com.whaleal.ddt.task.CommonTask;
import lombok.extern.log4j.Log4j2;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;

/**
 * @author: lhp
 * @time: 2021/7/21 2:38 下午
 * @desc: 读取event中的数据
 */
@Log4j2
public class ChangeStream extends CommonTask {
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
    private final boolean captureDDL;
    /**
     * event元数据库类保存数据信息的地方
     */
    private final MetadataEvent metadataEvent;
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

    public ChangeStream(String workName, String dsName, boolean captureDDL, String dbTableWhite, int startTimeOfOplog, int endTimeOfOplog, int delayTime) {
        super(workName, dsName);
        this.captureDDL = captureDDL;
        this.dbTableWhite = dbTableWhite;
        this.endTimeOfOplog = endTimeOfOplog;
        this.startTimeOfOplog = startTimeOfOplog;
        this.workName = workName;
        this.delayTime = delayTime;
        this.metadataEvent = MetadataEvent.getEventMetadata(workName);
        this.mongoClient = MongoDBConnectionSync.getMongoClient(dsName);
        this.dbVersion = MongoDBConnectionSync.getVersion(dsName);
    }


    @Override
    public void execute() {
        // 当出现异常时 可以进行进行查询
        while (!isReadScanOver) {
            log.info("{} ready to read oplog", workName);
            try {
                source();
            } catch (Exception e) {
                log.error("{} error reading oplog failed,msg:{}", workName, e.getMessage());
            }
        }
    }


    /**
     * source 读取oplog的中数据
     *
     * @desc 读取oplog的中数据
     */
    public void source() {
        BsonTimestamp docTime = new BsonTimestamp(startTimeOfOplog, 0);
        log.info("{} start reading changeStream data", workName);
        BasicDBObject condition = new BasicDBObject();
        if (startTimeOfOplog != 0) {
            // 设置查询数据的时间范围
            condition.append("clusterTime", new Document().append("$gte", docTime));
        }
        if (endTimeOfOplog != 0) {
            // 带有范围的oplog
            condition.append("clusterTime", new Document().append("$gte", docTime).append("$lte", new BsonTimestamp(endTimeOfOplog, 0)));
        }
        //   Q：读取全部数据，会造成带宽浪费
        //   A：增加正则表达式读取ns
        // 不为全部库表 todo 暂时没有表名单过滤
//        if (!(".+").equals(dbTableWhite)) {
//            condition.append("ns", new Document("$regex", dbTableWhite));
//        }

        log.info("{} the conditions for reading changeStream :{}", workName, condition.toJson());

        //
        int readNum = 1024000;
        try {
            List<Bson> pipeline = singletonList(
                    Aggregates.match(Filters.and(
                            condition
                    )));
            ChangeStreamIterable<Document> changeStream = mongoClient.watch(pipeline);

            if (String.valueOf(dbVersion.charAt(0)).compareTo("6") > 0) {
                changeStream.showExpandedEvents(true);
            }

            MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = changeStream.cursor();
            while (cursor.hasNext()) {
                ChangeStreamDocument<Document> changeEvent = cursor.next();

                if (changeEvent.getNamespace() == null) {
                    continue;
                }
                String ns = changeEvent.getNamespace().getFullName();
                // 10w条输出一次 或者10s输出一次
                if (readNum++ > 102400 || (changeEvent.getClusterTime().getTime() - lastOplogTs.getTime() > 60)) {
                    // 记录当前oplog的时间
                    lastOplogTs = changeEvent.getClusterTime();
                    docTime = changeEvent.getClusterTime();
                    readNum = 0;
                    log.info("{} current read changeStream time:{}", workName, lastOplogTs.getTime());
                    log.info("{} current oplog changeStream time:{} s", workName, Math.abs(System.currentTimeMillis() / 1000F - lastOplogTs.getTime()));
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
                        log.info("{} current incremental progress{}%", workName, percentage);
                    }
                    // 读取的第一条数据，一定会进来
                    // 判断是否在窗口期范围内
                    if (lastOplogTs.getTime() < startTimeOfOplog) {
                        log.error("{} failed to read oplog: missed sliding window time oplog is overwritten, this program is about to exit", workName);
                        WorkStatus.updateWorkStatus(workName, WorkStatus.WORK_STOP);
                    }
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

                // 过滤元数据库信息 local admin config库数据不同步
                if (!ns.startsWith("local.") && !ns.startsWith("admin.") && !ns.startsWith("config.")) {
                    // 保留本次oplog的ts读取时间
                    // 当前时间减去oplog的时间减去  小于延迟时间即可
                    if (delayTime > 0 && (System.currentTimeMillis() / 1000) - docTime.getTime() < delayTime) {
                        // 先把超时的时间进行睡眠等待
                        TimeUnit.SECONDS.sleep((System.currentTimeMillis() / 1000) - docTime.getTime());
                        TimeUnit.MINUTES.sleep(1);
                    }
                    metadataEvent.getQueueOfEvent().put(changeEvent);
                    metadataEvent.getReadNum().add(1);
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


}
