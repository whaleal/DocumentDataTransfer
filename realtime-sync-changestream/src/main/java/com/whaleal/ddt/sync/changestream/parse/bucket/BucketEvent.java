package com.whaleal.ddt.sync.changestream.parse.bucket;/*
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


import com.mongodb.client.MongoClient;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.whaleal.ddt.cache.BatchDataEntity;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.sync.changestream.cache.MetadataEvent;
import com.whaleal.ddt.sync.connection.MongoDBConnectionSync;
import com.whaleal.ddt.task.CommonTask;
import lombok.extern.log4j.Log4j2;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: lhp
 * @time: 2021/7/30 11:07 上午
 * @desc: 多个线程操作 此时要处理多个ns。每个ns，最多同时有一个线程处理。
 */
@Log4j2
public class BucketEvent extends CommonTask implements ParseEventInterface {


    public BucketEvent(String workName, String dsName, int maxBucketNum, Set<String> ddlSet, int ddlWait) {
        super(workName, dsName);
        this.metadataEvent = MetadataEvent.getEventMetadata(workName);
        this.maxBucketNum = maxBucketNum;
        this.ddlSet = ddlSet;
        this.workName = workName;
        this.ddlWait = ddlWait;
        this.mongoClient = MongoDBConnectionSync.getMongoClient(dsName);

    }

    /**
     * 等待ddl时间
     */
    protected final Integer ddlWait;
    /**
     * oplog元数据库
     */
    protected final MetadataEvent metadataEvent;
    /**
     * k桶号 默认[0-16)
     * v为Set<id>
     */
    protected final Map<Integer, Set<String>> bucketSetMap = new HashMap<>();
    /**
     * k桶号 默认[0-16)
     * v为解析好的WriteModel数据集合
     */
    protected final Map<Integer, List<WriteModel<Document>>> bucketWriteModelListMap = new HashMap<>();
    /**
     * mongoClient
     */
    protected MongoClient mongoClient;
    /**
     * 当前解析的表maxTableBatchNumOfBucket
     */
    protected String currentDbTable;
    /**
     * 桶个数
     */
    protected final int maxBucketNum;
    /**
     * 要同步的DDL列表
     */
    protected final Set<String> ddlSet;

    @Override
    public void execute() {
        log.info("{} the oplog bucketing thread starts running", workName);
        exe();
    }

    private void exe() {
        int idlingTime = 0;
        while (true) {
            try {
                // 判断任务状态
                if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_STOP) {
                    log.info("{} the {} thread exits from parsing the bucket", workName, Thread.currentThread().getName());
                    break;
                }
                if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_PAUSE) {
                    // 发生了限速就开始限制读取
                    while (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_PAUSE) {
                        TimeUnit.SECONDS.sleep(5);
                    }
                }
                /*
                 * 保存每个表的document
                 * k为表名，v为ns解析后的Document
                 */
                if (idlingTime++ > 10) {
                    // 10次都没有获取到oplog信息,则进行睡眠
                    TimeUnit.SECONDS.sleep(1);
                    // 10次都没有获得锁 更有可能继续无法获得'锁'
                    idlingTime = 9;
                }

                for (Map.Entry<String, BlockingQueue<ChangeStreamDocument<Document>>> next : metadataEvent.getQueueOfNsMap().entrySet()) {
                    // 表名
                    String dbTableName = next.getKey();
                    // 为空就不用进来处理了  也不用进行加锁信息
                    if (next.getValue().isEmpty()) {
                        idlingTime++;
                        continue;
                    }
                    // 当前线程要操作的表名进行赋值
                    this.currentDbTable = dbTableName;
                    // 加'锁',每个数据表最多同时有一个线程解析
                    AtomicBoolean atomicBoolean = metadataEvent.getStateOfNsMap().get(dbTableName);
                    boolean pre = atomicBoolean.get();
                    // cas操作
                    if (!pre && atomicBoolean.compareAndSet(false, true)) {
                        try {
                            // 队列数据
                            Queue<ChangeStreamDocument<Document>> documentQueue = next.getValue();
                            // 该表内有数据,idlingTime轮转次数设为0
                            idlingTime = 0;
                            // 对该表的bucketSetMap,bucketWriteModelListMap进行重新赋值
                            init();
                            // 解析队列中的数据
                            parse(documentQueue);
                            // 解析后把数据放入下一层级
                            putDataToCache();
                        } catch (Exception e) {
                            log.error("{} {} an error occurred in the bucketing thread of oplog, the error message:{}", workName, dbTableName, e.getMessage());
                        } finally {
                            // 释放'锁'
                            atomicBoolean.set(false);
                        }
                    }
                }
            } catch (Exception e) {
                log.error("{} an error occurred in the bucketing thread of oplog, the error message:{}", workName, e.getMessage());
            }
        }
    }

    public void parseDDL(ChangeStreamDocument<Document> changeStreamEvent) {
        try {
            // 当处理DDL时候 已经把所有数据推到下一层级
            String operationType = changeStreamEvent.getOperationTypeString();
            // todo 暂时放在这里 不处理
        } catch (Exception e) {
            log.error("{} failed to perform DDL operation:{},reason for failure:{}", workName, changeStreamEvent.toString(), e.getMessage());
        } finally {
            // 每次执行DDL 都要重新更新一下索引信息
            updateUniqueIndexCount(currentDbTable);
        }
    }

    /**
     * putDataToCache 添加数据到下一层级
     *
     * @desc 添加所有数据到下一层级
     */
    public void putDataToCache() {
        Set<Integer> keySet = bucketWriteModelListMap.keySet();
        for (Integer bucketNum : keySet) {
            putDataToCache(currentDbTable, bucketNum);
        }
    }

    /**
     * putDataToCache
     *
     * @param ns        库表民
     * @param bucketNum 桶号
     * @desc 添加数据到下一层级
     */
    public void putDataToCache(String ns, int bucketNum) {
        try {
            // 16个公共区
            int nsBucketNum = Math.abs((ns + bucketNum).hashCode() % maxBucketNum);
            if (metadataEvent.getUniqueIndexCollection().containsKey(ns)) {
                nsBucketNum = Math.abs(ns.hashCode() % maxBucketNum);
            }
            BatchDataEntity batchDataEntity = new BatchDataEntity();
            batchDataEntity.setNs(ns);
            batchDataEntity.setDataList(bucketWriteModelListMap.get(bucketNum));
            metadataEvent.getQueueOfBucketMap().get(nsBucketNum).put(batchDataEntity);
            // 修改map中的信息
            bucketSetMap.put(bucketNum, new HashSet<>());
            bucketWriteModelListMap.put(bucketNum, new ArrayList());
        } catch (Exception e) {
            log.error("{} an exception occurred when adding data to the oplogWrite thread, the error message:{}", workName, e.getMessage());
        }
    }

    /**
     * init
     *
     * @desc 初始化该表的信息
     */
    public void init() {
        for (int i = 0; i < maxBucketNum; i++) {
            bucketSetMap.put(i, new HashSet<>());
            bucketWriteModelListMap.put(i, new ArrayList());
        }
    }

    /**
     * com.whaleal.photon.source.mongodb.com.whaleal.ddt.parse
     *
     * @param documentQueue 数据队列
     * @desc 解析Document
     */
    public void parse(Queue<ChangeStreamDocument<Document>> documentQueue) {
        int parseSize = 0;
        while (true) {
            ChangeStreamDocument<Document> changeStreamEvent = null;
            try {
                changeStreamEvent = documentQueue.poll();
                // 当队列数据为0,或者当前表已经处理了8096条数据
                if (changeStreamEvent == null) {
                    break;
                }
                // todo
                String op = changeStreamEvent.getOperationTypeString();
                if ("insert".equals(op)) {
                    parseInsert(changeStreamEvent);
                } else if ("update".equals(op)) {
                    parseUpdate(changeStreamEvent);
                } else if ("replace".equals(op)) {
                    parseReplace(changeStreamEvent);
                } else if ("delete".equals(op)) {
                    parseDelete(changeStreamEvent);
                } else if ("updateIndexInfo".equals(op)) {
                    // 更新此表的唯一索引情况
                    updateUniqueIndexCount(currentDbTable);
                } else {
                    // 其他的都当做DDL处理
                    // 设置标识位：当前正在处理的DDL oplog
                    metadataEvent.getCurrentNsDealOplogInfo().put(currentDbTable, changeStreamEvent);
                    parseDDL(changeStreamEvent);
                    metadataEvent.updateBulkWriteInfo("cmd", 1);
                    metadataEvent.getCurrentNsDealOplogInfo().remove(currentDbTable);
                    updateUniqueIndexCount(currentDbTable);
                }
                // 一直有数据 就一直追加 此时大表中大幅度占有的时候 会阻塞其他线程的处理
                if (parseSize++ > 1024 * 10) {
                    break;
                }
            } catch (Exception e) {
                if (changeStreamEvent != null) {
                    log.error("{} an exception occurred while parsing the {} log, the error message:{}", workName, changeStreamEvent.toString(), e.getMessage());
                }
            }
        }
    }


    /**
     * createIndex 解析建立索引
     * todo 解析DDL
     *
     * @param document oplog数据
     * @desc 解析建立索引
     */
    public void createIndex(Document document) {

    }

    /**
     * parseDropIndex 解析删除索引
     *
     * @param document oplog数据
     * @desc 解析删除索引
     */
    public void dropIndex(ChangeStreamDocument<Document> changeStreamEvent) {

    }


    /**
     * parseDropTable 解析删表
     *
     * @param document oplog数据
     * @desc 解析删表
     */
    @Override
    public void parseDropTable(ChangeStreamDocument<Document> changeStreamEvent) {

    }

    /**
     * parseCreateTable 解析创建表
     *
     * @param document oplog数据
     * @desc 解析创建表
     */
    @Override
    public void parseCreateTable(ChangeStreamDocument<Document> changeStreamEvent) {

    }


    /**
     * parseRenameTable 解析表重命名
     *
     * @param document oplog数据
     * @desc 解析表重命名
     */
    @Override
    public void parseRenameTable(ChangeStreamDocument<Document> changeStreamEvent) {

    }


    /**
     * parseCreateIndex 解析建立索引
     *
     * @param document oplog数据
     * @desc 解析建立索引
     */
    @Override
    public void parseCreateIndex(ChangeStreamDocument<Document> changeStreamEvent) {

    }

    /**
     * parseDropIndex 解析删除索引
     *
     * @param document oplog数据
     * @desc 解析删除索引
     */
    @Override
    public void parseDropIndex(ChangeStreamDocument<Document> changeStreamEvent) {

    }

    /**
     * parseInsert 解析插入数据
     *
     * @param document oplog数据
     * @desc 解析插入数据
     */
    @Override
    public void parseInsert(ChangeStreamDocument<Document> changeStreamEvent) {
        String _id = changeStreamEvent.getDocumentKey().get("_id").toString();
        int bucketNum = Math.abs(_id.hashCode() % this.maxBucketNum);
        if (this.metadataEvent.getUniqueIndexCollection().containsKey(currentDbTable)) {
            bucketNum = 1;
        }
        // 检查该桶bucketSetMap是否存在。若不存在 则添加
        if (!this.bucketSetMap.get(bucketNum).add(_id)) {
            putDataToCache(this.currentDbTable, bucketNum);
            this.bucketSetMap.get(bucketNum).add(_id);
        }
        Document insertDocument = new Document();
        // 顺序不可写反
        insertDocument.putAll(changeStreamEvent.getFullDocument());
        insertDocument.putAll(changeStreamEvent.getDocumentKey());
        this.bucketWriteModelListMap.get(bucketNum).add(new InsertOneModel<Document>(insertDocument));
    }

    @Override
    public void parseUpdate(ChangeStreamDocument<Document> changeStreamEvent) {
        // todo 重新解析
//        String _id = changeStreamEvent.getDocumentKey().get("_id").toString();
//        int bucketNum = Math.abs(_id.hashCode() % maxBucketNum);
//        if (metadataEvent.getUniqueIndexCollection().containsKey(currentDbTable)) {
//            bucketNum = 1;
//        }
//        // 检查该桶bucketSetMap是否存在。若不存在 则添加
//        if (!bucketSetMap.get(bucketNum).add(_id)) {
//            putDataToCache(currentDbTable, bucketNum);
//            bucketSetMap.get(bucketNum).add(_id);
//        }
//        Document o2 = ((Document) document.get("o2"));
//        Document o = (Document) document.get("o");
//        o.remove("$v");
//        // 有些oplog的o没有$set和$unset的为Replace
//        if (o.get("$set") == null && o.get("$unset") == null) {
//            // 是否开启upsert
//            ReplaceOptions option = new ReplaceOptions();
//            option.upsert(true);
//            bucketWriteModelListMap.get(bucketNum).add(new ReplaceOneModel<Document>(o2, o, option));
//        } else {
//            bucketWriteModelListMap.get(bucketNum).add(new UpdateOneModel<Document>(o2, o));
//        }
    }

    @Override
    public void parseReplace(ChangeStreamDocument<Document> changeStreamEvent) {
        String _id = changeStreamEvent.getDocumentKey().get("_id").toString();
        int bucketNum = Math.abs(_id.hashCode() % this.maxBucketNum);
        if (this.metadataEvent.getUniqueIndexCollection().containsKey(currentDbTable)) {
            bucketNum = 1;
        }
        // 检查该桶bucketSetMap是否存在。若不存在 则添加
        if (!this.bucketSetMap.get(bucketNum).add(_id)) {
            putDataToCache(this.currentDbTable, bucketNum);
            this.bucketSetMap.get(bucketNum).add(_id);
        }

        BsonDocument filter = changeStreamEvent.getDocumentKey();
        Document fullDocument = changeStreamEvent.getFullDocument();
        fullDocument.putAll(filter);
        bucketWriteModelListMap.get(bucketNum).add(new ReplaceOneModel<>(filter, fullDocument));
    }

    /**
     * parseDelete 解析删除数据
     *
     * @param document oplog数据
     * @desc 解析删除数据
     */
    @Override
    public void parseDelete(ChangeStreamDocument<Document> changeStreamEvent) {
        String _id = changeStreamEvent.getDocumentKey().get("_id").toString();
        int bucketNum = Math.abs(_id.hashCode() % maxBucketNum);
        if (metadataEvent.getUniqueIndexCollection().containsKey(currentDbTable)) {
            bucketNum = 1;
        }
        // 检查该桶bucketSetMap是否存在。若不存在 则添加
        if (!bucketSetMap.get(bucketNum).add(_id)) {
            putDataToCache(currentDbTable, bucketNum);
            bucketSetMap.get(bucketNum).add(_id);
        }
        Document deleteDocument = new Document();
        deleteDocument.putAll(changeStreamEvent.getDocumentKey());
        DeleteOneModel<Document> deleteOneModel = new DeleteOneModel<Document>(deleteDocument);
        bucketWriteModelListMap.get(bucketNum).add(deleteOneModel);

    }

    @Override
    public void modifyCollection(ChangeStreamDocument<Document> changeStreamEvent) {

    }

    @Override
    public void shardCollection(ChangeStreamDocument<Document> changeStreamEvent) {

    }


    @Override
    public void updateUniqueIndexCount(String ns) {
        String[] nsSplit = ns.split("\\.", 2);
        int count = 0;
        try {
            for (Document index : mongoClient.getDatabase(nsSplit[0]).getCollection(nsSplit[1]).listIndexes()) {
                // 可以考虑一下unique的值为1
                if (index.containsKey("unique") && "true".equals(index.get("unique").toString())) {
                    count++;
                }
            }
            metadataEvent.getUniqueIndexCollection().put(ns, count);
        } catch (Exception e) {
            metadataEvent.getUniqueIndexCollection().put(ns, count + 1);
            log.error("{} failed to get {} collection index, the error message is:{}", workName, ns, e.getMessage());
        }
        // 如果表不存在唯一索引的话 可以进行删除此key 防止堆积太多ns
        if (metadataEvent.getUniqueIndexCollection().getOrDefault(ns, 0) == 0) {
            metadataEvent.getUniqueIndexCollection().remove(ns);
        }
    }
}
