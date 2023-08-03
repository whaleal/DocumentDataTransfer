/*
 * MongoT - An open-source project licensed under GPL+SSPL
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
package com.whaleal.ddt.parse.oplog;

import com.whaleal.ddt.cache.MetadataOplog;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.*;
import com.whaleal.ddt.cache.BatchDataEntity;
import com.whaleal.ddt.connection.MongoDBConnection;
import com.whaleal.ddt.util.ParserMongoStructureUtil;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.task.CommonTask;
import lombok.extern.log4j.Log4j2;
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
public abstract class BucketOplog extends CommonTask implements ParseOplogInterface {


    protected BucketOplog(String workName, String dsName, int maxBucketNum, Set<String> ddlSet, int ddlWait) {
        super(workName, dsName);
        this.metadataOplog = MetadataOplog.getOplogMetadata(workName);
        this.maxBucketNum = maxBucketNum;
        this.ddlSet = ddlSet;
        this.workName = workName;
        this.ddlWait = ddlWait;
        this.mongoClient = MongoDBConnection.getMongoClient(dsName);
    }

    /**
     * 等待ddl时间
     */
    protected final Integer ddlWait;
    /**
     * oplog元数据库
     */
    protected final MetadataOplog metadataOplog;
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
                    log.info("{} 线程{}退出执行解析bucket", workName, Thread.currentThread().getName());
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

                for (Map.Entry<String, BlockingQueue<Document>> next : metadataOplog.getQueueOfNsMap().entrySet()) {
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
                    AtomicBoolean atomicBoolean = metadataOplog.getStateOfNsMap().get(dbTableName);
                    boolean pre = atomicBoolean.get();
                    // cas操作
                    if (!pre && atomicBoolean.compareAndSet(false, true)) {
                        try {
                            // 队列数据
                            Queue<Document> documentQueue = next.getValue();
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

    public void parseDDL(Document document) {
        try {
            // 把数据推到下一层级
            putDataToCache();
            // 已经推送给下级的DDL 全部已经处理完
            metadataOplog.waitWriteData();
            // 当处理DDL时候 已经把所有数据推到下一层级
            Document o = (Document) document.get("o");
            // DDL 强制进行等待全部数据写入操作
            if (o.get(DROP_TABLE) != null && ddlSet.contains(DROP_TABLE)) {
                log.warn("{} drop table operation:{}", workName, document.toJson());
                parseDropTable(document);
            } else if (o.get(CREATE_TABLE) != null && ddlSet.contains(CREATE_TABLE)) {
                log.warn("{} create table operation:{}", workName, document.toJson());
                parseCreateTable(document);
            } else if (o.get(CREATE_INDEX) != null && ddlSet.contains(CREATE_INDEX)) {
                log.warn("{} create index operation:{}", workName, document.toJson());
                parseCreateIndex(document);
            } else if (o.get(COMMIT_INDEX_BUILD) != null && ddlSet.contains(CREATE_INDEX)) {
                log.warn("{} create index operation:{}", workName, document.toJson());
                parseCommitIndexBuild(document);
            } else if (o.get(DROP_INDEX) != null && ddlSet.contains(DROP_INDEX)) {
                log.warn("{} drop index operation:{}", workName, document.toJson());
                parseDropIndex(document);
            } else if (o.get(RENAME_COLLECTION) != null && ddlSet.contains(RENAME_COLLECTION)) {
                log.warn("{} rename table operation:{}", workName, document.toJson());
                parseRenameTable(document);
            } else if (o.get(CONVERT_TO_CAPPED) != null && ddlSet.contains(CONVERT_TO_CAPPED)) {
                log.warn("{} convert capped  table operation:{}", workName, document.toJson());
                parseConvertToCapped(document);
            } else if (o.get(DROP_DATABASE) != null && ddlSet.contains(DROP_DATABASE)) {
                log.warn("{} drop database operation:{}", workName, document.toJson());
            } else if (o.get(COLLECTION_MOD) != null && ddlSet.contains(COLLECTION_MOD)) {
                log.warn("{} collMod table operation:{}", workName, document.toJson());
                parseCollMod(document);
            }
        } catch (Exception e) {
            log.error("{} failed to perform DDL operation:{},reason for failure:{}", workName, document.toJson(), e.getMessage());
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
            if (metadataOplog.getUniqueIndexCollection().containsKey(ns)) {
                nsBucketNum = Math.abs(ns.hashCode() % maxBucketNum);
            }
            BatchDataEntity batchDataEntity = new BatchDataEntity();
            batchDataEntity.setNs(ns);
            batchDataEntity.setDataList(bucketWriteModelListMap.get(bucketNum));
            metadataOplog.getQueueOfBucketMap().get(nsBucketNum).put(batchDataEntity);
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
    public void parse(Queue<Document> documentQueue) {
        int parseSize = 0;
        while (true) {
            Document document = null;
            try {
                document = documentQueue.poll();
                // 当队列数据为0,或者当前表已经处理了8096条数据
                if (document == null) {
                    break;
                }
                String op = document.get("op").toString();
                if ("i".equals(op)) {
                    parseInsert(document);
                } else if ("u".equals(op)) {
                    parseUpdate(document);
                } else if ("d".equals(op)) {
                    parseDelete(document);
                } else if ("c".equals(op)) {
                    // 设置标识位：当前正在处理的DDL oplog
                    metadataOplog.getCurrentNsDealOplogInfo().put(currentDbTable, document);
                    parseDDL(document);
                    metadataOplog.updateBulkWriteInfo("cmd", 1);
                    metadataOplog.getCurrentNsDealOplogInfo().remove(currentDbTable);
                    updateUniqueIndexCount(currentDbTable);
                } else if ("updateIndexInfo".equals(op)) {
                    // 更新此表的唯一索引情况
                    updateUniqueIndexCount(currentDbTable);
                }
                // 一直有数据 就一直追加 此时大表中大幅度占有的时候 会阻塞其他线程的处理
                if (parseSize++ > 1024 * 10) {
                    break;
                }
            } catch (Exception e) {
                if (document != null) {
                    log.error("{} an exception occurred while parsing the {} log, the error message:{}", workName, document.toJson(), e.getMessage());
                }
            }
        }
    }

    /**
     * parseCreateIndex 解析建立索引
     *
     * @param document oplog数据
     * @desc 解析建立索引  把 CommitIndexBuild转为普通方式建立索引
     */
    public void parseCommitIndexBuild(Document document) {
        Document o = (Document) document.get("o");
        // todo 考虑commitIndexBuild要不要进行建立索引 可能会出现撤回建立的情况
        String tableName = o.get("commitIndexBuild").toString();
        List<Document> indexes = o.getList("indexes", Document.class);
        for (Document doc : indexes) {
            // 把commitIndexBuild转为普通方式建立索引
            Document newOplogDoc = new Document();
            newOplogDoc.append("ns", document.get("ns"));
            Document newOplogDoc_o = new Document();
            newOplogDoc_o.append("createIndexes", tableName);
            newOplogDoc_o.putAll(doc);
            newOplogDoc.append("o", newOplogDoc_o);
            newOplogDoc.append("ts", document.get("ts"));
            createIndex(newOplogDoc.append("createIndex", true));
        }
    }

    /**
     * createIndex 解析建立索引
     *
     * @param document oplog数据
     * @desc 解析建立索引
     */
    public void createIndex(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("createIndexes").toString();

        BasicDBObject index = new BasicDBObject();
        Document key = (Document) o.get("key");
        for (Map.Entry<String, Object> indexTemp : key.entrySet()) {
            index.append(indexTemp.getKey(), indexTemp.getValue());
        }
        IndexOptions indexOptions = ParserMongoStructureUtil.parseIndexOptions(o);
        {
            boolean isExist = false;
            // 先检查这个表在目标段是否真的存在
            for (String next : mongoClient.getDatabase(dbName).listCollectionNames()) {
                if (next.equals(tableName)) {
                    isExist = true;
                    break;
                }
            }
            if (isExist) {
                // 这里的线程 用完直接删除 即可
                Thread createIndexThread = new Thread(() -> {
                    long startCreateIndexTime = System.currentTimeMillis();
                    try {
                        indexOptions.background(true);
                        mongoClient.getDatabase(dbName).getCollection(tableName).createIndex(index, indexOptions);
                    } catch (Exception e) {
                        log.error("{} failed to build index:[{}],msg:{}", workName, document.toJson(), e.getMessage());
                    } finally {
                        log.info("{} build index:[{}],use time:{}s", workName, document.toJson(), (System.currentTimeMillis() - startCreateIndexTime) / 1000);
                    }
                });
                createIndexThread.setName(workName + "_realTimeCreateIndex_" + System.currentTimeMillis());
                createIndexThread.start();
                try {
                    // 最多等待10分钟
                    createIndexThread.join(Math.min(1000L * Math.round(ddlWait * 0.8F), 1000L * 600));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    /**
     * parseDropIndex 解析删除索引
     *
     * @param document oplog数据
     * @desc 解析删除索引
     */
    public void dropIndex(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("dropIndexes").toString();
        String indexName = o.get("index").toString();
        try {
            mongoClient.getDatabase(dbName).getCollection(tableName).dropIndex(indexName);
        } catch (Exception e) {
            log.error("{} failed to drop index:[{}],msg:{}", workName, document.toJson(), e.getMessage());
        }
    }


    /**
     * parseDropTable 解析删表
     *
     * @param document oplog数据
     * @desc 解析删表
     */
    @Override
    public void parseDropTable(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("drop").toString();
        mongoClient.getDatabase(dbName).getCollection(tableName).drop();
    }

    /**
     * parseCreateTable 解析创建表
     *
     * @param document oplog数据
     * @desc 解析创建表
     */
    @Override
    public void parseCreateTable(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("create").toString();
        CreateCollectionOptions collectionOptions = ParserMongoStructureUtil.parseCreateCollectionOption(o);
        // todo 考虑一下 是否合理 是否强制删除
        if (ddlSet.contains(DROP_TABLE)) {
            // 建表前已经删表
            mongoClient.getDatabase(dbName).getCollection(tableName).drop();
        }
        // 正式建表
        mongoClient.getDatabase(dbName).createCollection(tableName, collectionOptions);
    }


    /**
     * parseRenameTable 解析表重命名
     *
     * @param document oplog数据
     * @desc 解析表重命名
     */
    @Override
    public void parseRenameTable(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String dbNameAndTableName = o.get("renameCollection").toString();
        String tableName = dbNameAndTableName.split("\\.", 2)[1];
        String newTableName = o.get("to").toString().split("\\.", 2)[1];
        MongoNamespace mongoNamespace = new MongoNamespace(dbName + "." + newTableName);
        RenameCollectionOptions renameCollectionOptions = new RenameCollectionOptions();
        renameCollectionOptions.dropTarget(false);
        // 如果目标表已经存在,是否进行删除
        if (o.get("dropTarget") != null && "true".equalsIgnoreCase(o.get("dropTarget").toString())) {
            renameCollectionOptions.dropTarget(true);
        }
        if (this.ddlSet.contains(RENAME_COLLECTION)) {
            //  todo 是否合理 强制进行删除和重命名
            renameCollectionOptions.dropTarget(true);
        }
        this.mongoClient.getDatabase(dbName).getCollection(tableName).renameCollection(mongoNamespace, renameCollectionOptions);
        // 更新原表和新表的索引信息
        updateUniqueIndexCount(dbName + "." + tableName);
        updateUniqueIndexCount(dbName + "." + newTableName);
    }


    /**
     * parseCreateIndex 解析建立索引
     *
     * @param document oplog数据
     * @desc 解析建立索引
     */
    @Override
    public void parseCreateIndex(Document document) {
        createIndex(document.append("createIndex", true));
    }

    /**
     * parseDropIndex 解析删除索引
     *
     * @param document oplog数据
     * @desc 解析删除索引
     */
    @Override
    public void parseDropIndex(Document document) {
        dropIndex(document.append("dropIndex", true));
    }

    /**
     * parseInsert 解析插入数据
     *
     * @param document oplog数据
     * @desc 解析插入数据
     */
    @Override
    public void parseInsert(Document document) {
        String _id = ((Document) document.get("o")).get("_id").toString();
        int bucketNum = Math.abs(_id.hashCode() % this.maxBucketNum);
        if (this.metadataOplog.getUniqueIndexCollection().containsKey(currentDbTable)) {
            bucketNum = 1;
        }
        // 检查该桶bucketSetMap是否存在。若不存在 则添加
        if (!this.bucketSetMap.get(bucketNum).add(_id)) {
            putDataToCache(this.currentDbTable, bucketNum);
            this.bucketSetMap.get(bucketNum).add(_id);
        }
        Document insertDocument = (Document) document.get("o");
        // fromMigrate要特殊处理      shared准备
        if (document.get("fromMigrate") == null || !document.getBoolean("fromMigrate")) {
            this.bucketWriteModelListMap.get(bucketNum).add(new InsertOneModel<Document>(insertDocument));
        } else {
            // 是否开启upsert
            // 为shared准备
            ReplaceOptions option = new ReplaceOptions();
            option.upsert(true);
            this.bucketWriteModelListMap.get(bucketNum).add(new ReplaceOneModel<Document>(insertDocument, insertDocument, option));
        }
    }

    /**
     * parseDelete 解析删除数据
     *
     * @param document oplog数据
     * @desc 解析删除数据
     */
    @Override
    public void parseDelete(Document document) {
        String _id = ((Document) document.get("o")).get("_id").toString();
        int bucketNum = Math.abs(_id.hashCode() % maxBucketNum);
        if (metadataOplog.getUniqueIndexCollection().containsKey(currentDbTable)) {
            bucketNum = 1;
        }
        // 检查该桶bucketSetMap是否存在。若不存在 则添加
        if (!bucketSetMap.get(bucketNum).add(_id)) {
            putDataToCache(currentDbTable, bucketNum);
            bucketSetMap.get(bucketNum).add(_id);
        }
        Document deleteDocument = (Document) document.get("o");
        DeleteOneModel<Document> deleteOneModel = new DeleteOneModel<Document>(deleteDocument);
        // fromMigrate要特殊处理
        if (document.get("fromMigrate") == null || !document.getBoolean("fromMigrate")) {
            bucketWriteModelListMap.get(bucketNum).add(deleteOneModel);
        }
    }

    @Override
    public void parseConvertToCapped(Document document) {
        // 暂时不处理 此命令会才分为 :建表和重命名 两条oplog日志
    }

    /**
     * parseDropDatabase 删库
     *
     * @param document oplog数据
     * @desc 删库。
     */
    @Override
    public void parseDropDatabase(Document document) {
        // 暂时不处理 前序操作已经执行
    }

    /**
     * parseCollMod 表结构修改
     *
     * @param document oplog数据
     * @desc 表结构修改。
     */
    @Override
    public void parseCollMod(Document document) {
        // 表结构修改
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        mongoClient.getDatabase(dbName).runCommand(o);
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
            metadataOplog.getUniqueIndexCollection().put(ns, count);
        } catch (Exception e) {
            metadataOplog.getUniqueIndexCollection().put(ns, count + 1);
            log.error("{} failed to get {} collection index, the error message is:{}", workName, ns, e.getMessage());
        }
        // 如果表不存在唯一索引的话 可以进行删除此key 防止堆积太多ns
        if (metadataOplog.getUniqueIndexCollection().getOrDefault(ns, 0) == 0) {
            metadataOplog.getUniqueIndexCollection().remove(ns);
        }
    }
}
