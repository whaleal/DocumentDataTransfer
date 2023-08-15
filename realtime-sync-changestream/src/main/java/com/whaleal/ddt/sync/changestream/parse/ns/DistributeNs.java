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
package com.whaleal.ddt.sync.changestream.parse.ns;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.sync.changestream.cache.MetadataEvent;
import com.whaleal.ddt.sync.connection.MongoDBConnectionSync;
import com.whaleal.ddt.task.CommonTask;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author: lhp
 * @time: 2021/7/21 2:38 下午
 * @desc: 解析document的ns
 */
@Log4j2
public class DistributeNs extends CommonTask {
    /**
     * Event元数据库类
     */
    private final MetadataEvent metadataEvent;
    /**
     * 表过滤策略 正则表达式处理,着重处理ddl操作表
     */
    private final String dbTableWhite;
    /**
     * mongoClient
     */
    private final MongoClient mongoClient;
    /**
     * 每个ns队列最大缓存个数
     */
    private final int maxQueueSizeOfNs;


    public DistributeNs(String workName, String dbTableWhite, String dsName, int maxQueueSizeOfNs) {
        super(workName, dsName);
        this.dbTableWhite = dbTableWhite;
        this.workName = workName;
        this.metadataEvent = MetadataEvent.getEventMetadata(workName);
        this.mongoClient = MongoDBConnectionSync.getMongoClient(dsName);
        this.maxQueueSizeOfNs = maxQueueSizeOfNs;
    }

    @Override
    public void execute() {
        log.info("{} changeStream parsing ns thread starts running", workName);
        // 当前解析oplog日志的个数
        exe();
    }

    private void exe() {
        int count = 0;
        // 上次清除ns的时间
        long lastCleanNsTime = System.currentTimeMillis();
        while (true) {
            // 要加上异常处理 以防出现解析ns的线程异常退出
            ChangeStreamDocument<Document> changeStreamEvent = null;
            try {
                // 从原始的eventList进行解析，获取对应的ns
                changeStreamEvent = metadataEvent.getQueueOfEvent().poll();
                if (changeStreamEvent != null) {
                    // 解析ns
                    parseNs(changeStreamEvent);
                } else {
                    // 要是oplog太慢,count增加,lastCleanNsTime减少,此时也及时进行清除ns。
                    count += 1000;
                    lastCleanNsTime -= 1000;
                    // 代表changeStream队列为空 暂时休眠
                    TimeUnit.SECONDS.sleep(1);
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
                // 每100w条 && 10分钟 清除一下空闲ns表信息
                if (count++ > 1000000) {
                    count = 0;
                    long currentTimeMillis = System.currentTimeMillis();
                    // 10分钟
                    if ((currentTimeMillis - lastCleanNsTime) > 1000 * 60 * 10) {
                        lastCleanNsTime = currentTimeMillis;
                        log.warn("{} start removing redundant ns buckets", workName);
                        for (Map.Entry<String, BlockingQueue<ChangeStreamDocument<Document>>> queueEntry : metadataEvent.getQueueOfNsMap().entrySet()) {
                            try {
                                BlockingQueue<ChangeStreamDocument<Document>> value = queueEntry.getValue();
                                String key = queueEntry.getKey();
                                AtomicBoolean atomicBoolean = metadataEvent.getStateOfNsMap().get(key);
                                boolean pre = atomicBoolean.get();
                                // cas操作
                                if (value.isEmpty() && !pre && atomicBoolean.compareAndSet(false, true)) {
                                    metadataEvent.getQueueOfNsMap().remove(key);
                                    metadataEvent.getStateOfNsMap().remove(key);
                                    atomicBoolean.set(false);
                                }
                            } catch (Exception e) {
                                log.error("{} error in clearing free ns queue of oplog,msg:{}", workName, e.getMessage());
                            }
                        }
                    }
                }
            } catch (Exception e) {
                if (changeStreamEvent != null) {
                    log.error("{} currently parsing the changeStream log:{}", workName, changeStreamEvent.toString());
                }
                log.error("{} an error occurred in the split table thread of the changeStream,msg:{}", workName, e.getMessage());
            }
        }
    }

    /**
     * parseNs
     *
     * @desc 解析document的ns
     */
    public void parseNs(ChangeStreamDocument<Document> changeStreamEvent) throws InterruptedException {
        // getFullName 已在上级进行判断了，不会出现空指针
        String fullDbTableName = changeStreamEvent.getNamespace().getFullName();
        String op = changeStreamEvent.getOperationTypeString();
        boolean isDDL = false;
        // DDL  判断那些DDL名称
        if ("create".equals(op) ||
                "createIndexes".equals(op) ||
                "drop".equals(op) ||
                "dropDatabase".equals(op) ||
                "dropIndexes".equals(op) ||
                "rename".equals(op) ||
                "modify".equals(op) ||
                "shardCollection".equals(op)) {
            isDDL = true;
        }
        String tableName = changeStreamEvent.getNamespace().getCollectionName();
        // todo 这一款需要修改 调研日志
        // system.buckets.
        // 5.0以后分桶表 可以存储数据 可以参考system.txt说明
        if (tableName.startsWith("system.") && (!tableName.startsWith("system.buckets."))) {
            return;
        }
        // 多重DDL 保证DDL顺序性问题
        if (isDDL) {
            metadataEvent.waitCacheExe();
        }
        if (!metadataEvent.getQueueOfNsMap().containsKey(fullDbTableName)) {
            metadataEvent.getQueueOfNsMap().put(fullDbTableName, new LinkedBlockingQueue<>(maxQueueSizeOfNs));
            metadataEvent.getStateOfNsMap().put(fullDbTableName, new AtomicBoolean());
            // 更新此表的唯一索引情况
            Document updateIndexInfo = new Document();
            updateIndexInfo.put("op", "updateIndexInfo");
            metadataEvent.getQueueOfNsMap().get(fullDbTableName).put(changeStreamEvent);
            // 保证DDL顺序性问题
            metadataEvent.waitCacheExe();
        }
        metadataEvent.getQueueOfNsMap().get(fullDbTableName).put(changeStreamEvent);
        if (isDDL) {
            metadataEvent.waitCacheExe();
        }
    }

}
