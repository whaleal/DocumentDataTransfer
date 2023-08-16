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

package com.whaleal.ddt.sync.changestream.distribute.bucket;

import com.mongodb.MongoNamespace;
import com.mongodb.client.model.*;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.UpdateDescription;
import com.whaleal.ddt.realtime.common.distribute.bucket.BaseDistributeBucket;
import lombok.extern.log4j.Log4j2;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.Queue;
import java.util.Set;

/**
 * 分布式桶操作类，用于多个线程操作。每个ns（命名空间）最多同时有一个线程处理。
 *
 * @param <T> ChangeStreamDocument类型的泛型参数。
 */
@Log4j2
public class DistributeBucket extends BaseDistributeBucket<ChangeStreamDocument<Document>> {

    /**
     * 构造函数，初始化基本参数和MongoClient
     *
     * @param workName     工作名称
     * @param sourceDsName source数据源名称
     * @param targetDsName target数据源名称
     * @param maxBucketNum 最大桶数量
     * @param ddlSet       DDL集合
     * @param ddlWait      等待DDL的时间
     */
    public DistributeBucket(String workName, String sourceDsName, String targetDsName, int maxBucketNum, Set<String> ddlSet, int ddlWait) {
        super(workName, sourceDsName,targetDsName, maxBucketNum, ddlSet, ddlWait);
    }

    @Override
    public void execute() {
        log.info("{} the changeStream bucketing thread starts running", workName);
        exe();
    }

    @Override
    public void parseDDL(ChangeStreamDocument<Document> changeStreamEvent) {
        try {
            // 当处理DDL时候 已经把所有数据推到下一层级
            String operationType = changeStreamEvent.getOperationTypeString();
            log.warn("{} {} perform DDL operations {}:{}", workName, changeStreamEvent.getNamespace().getFullName(), changeStreamEvent.getOperationTypeString(), changeStreamEvent.toString());
            // todo 暂时放在这里 不处理
            switch (operationType) {
                case CREATE_TABLE:
                    parseCreateTable(changeStreamEvent);
                    break;
                case DROP_TABLE:
                    parseDropTable(changeStreamEvent);
                    break;
                case CREATE_INDEX:
                    parseCreateIndex(changeStreamEvent);
                    break;
                case DROP_INDEX:
                    parseDropIndex(changeStreamEvent);
                    break;
                case RENAME:
                    parseRenameTable(changeStreamEvent);
                    break;
                case MODIFY_COLLECTION:
                    modifyCollection(changeStreamEvent);
                    break;
                case SHARD_COLLECTION:
                    shardCollection(changeStreamEvent);
                    break;
                default:
                    // Handle default case if needed
                    break;
            }
        } catch (Exception e) {
            log.error("{} failed to perform DDL operation:{},reason for failure:{}", workName, changeStreamEvent.toString(), e.getMessage());
        } finally {
            // 每次执行DDL 都要重新更新一下索引信息
            updateUniqueIndexCount(currentDbTable);
        }
    }


    @Override
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
                String op = changeStreamEvent.getOperationTypeString();
                switch (op) {
                    case "insert":
                        parseInsert(changeStreamEvent);
                        break;
                    case "update":
                        parseUpdate(changeStreamEvent);
                        break;
                    case "replace":
                        parseReplace(changeStreamEvent);
                        break;
                    case "delete":
                        parseDelete(changeStreamEvent);
                        break;
                    case "updateIndexInfo":
                        // 更新此表的唯一索引情况
                        updateUniqueIndexCount(currentDbTable);
                        break;
                    default:
                        // 其他的都当做DDL处理
                        // 设置标识位：当前正在处理的DDL oplog
                        metadata.getCurrentNsDealEventInfo().put(currentDbTable, changeStreamEvent);
                        parseDDL(changeStreamEvent);
                        metadata.updateBulkWriteInfo("cmd", 1);
                        metadata.getCurrentNsDealEventInfo().remove(currentDbTable);
                        updateUniqueIndexCount(currentDbTable);
                        break;
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


    @Override
    public void parseDropTable(ChangeStreamDocument<Document> changeStreamEvent) {
        MongoNamespace namespace = changeStreamEvent.getNamespace();
        targetMongoClient.getDatabase(namespace.getDatabaseName()).getCollection(namespace.getCollectionName()).drop();
    }


    @Override
    public void parseCreateTable(ChangeStreamDocument<Document> changeStreamEvent) {
        // todo 未实现
    }


    @Override
    public void parseRenameTable(ChangeStreamDocument<Document> changeStreamEvent) {
        MongoNamespace oldNs = changeStreamEvent.getNamespace();
        RenameCollectionOptions renameCollectionOptions = new RenameCollectionOptions();
        renameCollectionOptions.dropTarget(false);
        // 如果目标表已经存在,是否进行删除
        if (this.ddlSet.contains(RENAME_COLLECTION)) {
            //  q: 是否合理 强制进行删除和重命名
            //  a: 当用户允许使用rename时,就强制删除目标段已经存在的表
            renameCollectionOptions.dropTarget(true);
        }
        this.targetMongoClient.getDatabase(oldNs.getDatabaseName()).getCollection(oldNs.getCollectionName()).renameCollection(changeStreamEvent.getDestinationNamespace(), renameCollectionOptions);
        // 更新原表和新表的索引信息
        updateUniqueIndexCount(oldNs.getFullName());
        updateUniqueIndexCount(changeStreamEvent.getDestinationNamespace().getFullName());
    }


    @Override
    public void parseCreateIndex(ChangeStreamDocument<Document> changeStreamEvent) {
        // todo 未实现
    }


    @Override
    public void parseDropIndex(ChangeStreamDocument<Document> changeStreamEvent) {
        // todo 未实现
    }


    @Override
    public void parseInsert(ChangeStreamDocument<Document> changeStreamEvent) {
        String _id = changeStreamEvent.getDocumentKey().get("_id").toString();
        int bucketNum = Math.abs(_id.hashCode() % this.maxBucketNum);
        if (this.metadata.getUniqueIndexCollection().containsKey(currentDbTable)) {
            bucketNum = 1;
        }
        // 检查该桶bucketSetMap是否存在。若不存在 则添加
        if (!this.bucketSetMap.get(bucketNum).add(_id)) {
            putDataToCache(this.currentDbTable, bucketNum);
            this.bucketSetMap.get(bucketNum).add(_id);
        }
        Document insertDocument = changeStreamEvent.getFullDocument();
        this.bucketWriteModelListMap.get(bucketNum).add(new InsertOneModel<>(insertDocument));
    }

    @Override
    public void parseUpdate(ChangeStreamDocument<Document> changeStreamEvent) {
        String _id = changeStreamEvent.getDocumentKey().get("_id").toString();
        int bucketNum = Math.abs(_id.hashCode() % maxBucketNum);
        if (metadata.getUniqueIndexCollection().containsKey(currentDbTable)) {
            bucketNum = 1;
        }
        // 检查该桶bucketSetMap是否存在。若不存在 则添加
        if (!bucketSetMap.get(bucketNum).add(_id)) {
            putDataToCache(currentDbTable, bucketNum);
            bucketSetMap.get(bucketNum).add(_id);
        }
        UpdateDescription updateDescription = changeStreamEvent.getUpdateDescription();

        Document set = new Document();
        set.putAll(updateDescription.getUpdatedFields());
        Document unset = new Document();

        for (String removedField : updateDescription.getRemovedFields()) {
            unset.append(removedField, null);
        }
        Document update = new Document();
        if (set.size() > 0) {
            update.append("$set", set);
        }
        if (unset.size() > 0) {
            update.append("$unset", set);
        }
        // 一定会出现$set||$unset
        bucketWriteModelListMap.get(bucketNum).add(new UpdateOneModel<Document>(changeStreamEvent.getDocumentKey(), update));
    }

    @Override
    public void parseReplace(ChangeStreamDocument<Document> changeStreamEvent) {
        String _id = changeStreamEvent.getDocumentKey().get("_id").toString();
        int bucketNum = Math.abs(_id.hashCode() % this.maxBucketNum);
        if (this.metadata.getUniqueIndexCollection().containsKey(currentDbTable)) {
            bucketNum = 1;
        }
        // 检查该桶bucketSetMap是否存在。若不存在 则添加
        if (!this.bucketSetMap.get(bucketNum).add(_id)) {
            putDataToCache(this.currentDbTable, bucketNum);
            this.bucketSetMap.get(bucketNum).add(_id);
        }
        BsonDocument filter = changeStreamEvent.getDocumentKey();
        Document fullDocument = changeStreamEvent.getFullDocument();
        bucketWriteModelListMap.get(bucketNum).add(new ReplaceOneModel<>(filter, fullDocument));
    }


    @Override
    public void parseDelete(ChangeStreamDocument<Document> changeStreamEvent) {
        String _id = changeStreamEvent.getDocumentKey().get("_id").toString();
        int bucketNum = Math.abs(_id.hashCode() % maxBucketNum);
        if (metadata.getUniqueIndexCollection().containsKey(currentDbTable)) {
            bucketNum = 1;
        }
        // 检查该桶bucketSetMap是否存在。若不存在 则添加
        if (!bucketSetMap.get(bucketNum).add(_id)) {
            putDataToCache(currentDbTable, bucketNum);
            bucketSetMap.get(bucketNum).add(_id);
        }
        BsonDocument deleteDocument = changeStreamEvent.getDocumentKey();
        DeleteOneModel<Document> deleteOneModel = new DeleteOneModel<>(deleteDocument);
        bucketWriteModelListMap.get(bucketNum).add(deleteOneModel);
    }

    @Override
    public void parseConvertToCapped(ChangeStreamDocument<Document> event) {
        // changeStream无该方法
    }

    @Override
    public void parseDropDatabase(ChangeStreamDocument<Document> event) {
        // 可以不use该方法
    }

    @Override
    public void parseCollMod(ChangeStreamDocument<Document> event) {
        // changeStream无该方法
    }

    @Override
    public void modifyCollection(ChangeStreamDocument<Document> changeStreamEvent) {
        // todo 未实现
    }

    @Override
    public void shardCollection(ChangeStreamDocument<Document> changeStreamEvent) {
        // todo 未实现
    }
}
