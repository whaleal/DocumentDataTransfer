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
package com.whaleal.ddt.sync.distribute.bucket;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.*;
import com.whaleal.ddt.realtime.common.distribute.bucket.BaseDistributeBucket;
import com.whaleal.ddt.util.ParserMongoStructureUtil;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * DistributeBucket类是一个抽象类，它继承了BaseDistributeBucket类，并提供了对MongoDB数据库操作的实现。
 * 它主要用于处理多线程操作，每个线程处理一个命名空间（ns）。
 * 它提供了对数据库操作（如插入、更新、删除等）的解析方法，以及对DDL操作（如创建表、删除表、创建索引等）的解析方法。
 */
@Log4j2

public abstract class DistributeBucket extends BaseDistributeBucket<Document> {

    /**
     * 构造函数
     *
     * @param workName     工作名称
     * @param dsName       数据源名称
     * @param maxBucketNum 最大桶数
     * @param ddlSet       数据定义语言集合
     * @param ddlWait      数据定义语言等待时间
     */
    protected DistributeBucket(String workName, String dsName, int maxBucketNum, Set<String> ddlSet, int ddlWait) {
        super(workName, dsName, maxBucketNum, ddlSet, ddlWait);
    }


    @Override
    public void execute() {
        log.info("{} the event bucketing thread starts running", workName);
        exe();
    }


    @Override
    public void parseDDL(Document document) {
        try {
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


    @Override
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
                    metadata.getCurrentNsDealEventInfo().put(currentDbTable, document);
                    parseDDL(document);
                    metadata.updateBulkWriteInfo("cmd", 1);
                    metadata.getCurrentNsDealEventInfo().remove(currentDbTable);
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
     * parseUpdate
     *
     * @param document oplog数据
     * @desc 解析建立索引  把 CommitIndexBuild转为普通方式建立索引
     */
    private void parseCommitIndexBuild(Document document) {
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
    private void createIndex(Document document) {
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
    private void dropIndex(Document document) {
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


    @Override
    public void parseDropTable(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("drop").toString();
        mongoClient.getDatabase(dbName).getCollection(tableName).drop();
    }

    @Override
    public void parseCreateTable(Document document) {
        String ns = document.get("ns").toString();
        String[] nsSplit = ns.split("\\.", 2);
        String dbName = nsSplit[0];
        Document o = (Document) document.get("o");
        String tableName = o.get("create").toString();
        CreateCollectionOptions collectionOptions = ParserMongoStructureUtil.parseCreateCollectionOption(o);
        if (ddlSet.contains(DROP_TABLE)) {
            // 建表前已经删表
            mongoClient.getDatabase(dbName).getCollection(tableName).drop();
        }
        // 正式建表 可能会出现建标失败，但是有日志捕获
        mongoClient.getDatabase(dbName).createCollection(tableName, collectionOptions);
    }


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
            //  q: 是否合理 强制进行删除和重命名
            //  a: 当用户允许使用rename时,就强制删除目标段已经存在的表
            renameCollectionOptions.dropTarget(true);
        }
        this.mongoClient.getDatabase(dbName).getCollection(tableName).renameCollection(mongoNamespace, renameCollectionOptions);
        // 更新原表和新表的索引信息
        updateUniqueIndexCount(dbName + "." + tableName);
        updateUniqueIndexCount(dbName + "." + newTableName);
    }


    @Override
    public void parseCreateIndex(Document document) {
        createIndex(document.append("createIndex", true));
    }


    @Override
    public void parseDropIndex(Document document) {
        dropIndex(document.append("dropIndex", true));
    }


    @Override
    public void parseInsert(Document document) {
        String _id = ((Document) document.get("o")).get("_id").toString();
        int bucketNum = Math.abs(_id.hashCode() % this.maxBucketNum);
        if (this.metadata.getUniqueIndexCollection().containsKey(currentDbTable)) {
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


    @Override
    public void parseDelete(Document document) {
        String _id = ((Document) document.get("o")).get("_id").toString();
        int bucketNum = Math.abs(_id.hashCode() % maxBucketNum);
        if (metadata.getUniqueIndexCollection().containsKey(currentDbTable)) {
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
        // 暂时不处理 此命令会分为 :建表和重命名 两条oplog日志
    }


    @Override
    public void parseDropDatabase(Document document) {
        // 暂时不处理 前序操作已经执行
    }


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
    public void modifyCollection(Document event) {
        // oplog无该方法用不到
    }

    @Override
    public void shardCollection(Document event) {
        // oplog无该方法用不到
    }

    @Override
    public void parseReplace(Document event) {
        // oplog无该方法用不到
    }

}
