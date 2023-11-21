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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

/**
 * 分布式桶操作类，用于多个线程操作。每个ns（命名空间）最多同时有一个线程处理。
 *
 * @param <Document> ChangeStreamDocument类型的泛型参数。
 */
@Log4j2
public class DistributeBucketOfDouble extends BaseDistributeBucket<ChangeStreamDocument<Document>> {


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
    public DistributeBucketOfDouble(String workName, String sourceDsName, String targetDsName, int maxBucketNum, Set<String> ddlSet, int ddlWait) {
        super(workName, sourceDsName, targetDsName, maxBucketNum, ddlSet, ddlWait);
    }

    @Override
    public void execute() {
        log.info("{} the changeStream bucketing thread starts running", workName);
        exe();
    }

    @Override
    public void parseDDL(ChangeStreamDocument<Document> changeStreamEvent) {
        // 当处理DDL时候 已经把所有数据推到下一层级
        String operationType = changeStreamEvent.getOperationTypeString();
        if (!ddlSet.contains(operationType)) {
            return;
        }
        try {

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
        if (this.bucketSetMap.get(bucketNum).containsKey(_id)) {
            if (!filter(_id, bucketNum, changeStreamEvent, "insert")) {
                return;
            }
        }
        bucketSetMap.get(bucketNum).put(_id, this.bucketWriteModelListMap.get(bucketNum).size());
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
        if (bucketSetMap.get(bucketNum).containsKey(_id)) {
            if (!filter(_id, bucketNum, changeStreamEvent, "update")) {
                return;
            }
        }
        bucketSetMap.get(bucketNum).put(_id, this.bucketWriteModelListMap.get(bucketNum).size());
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
            update.append("$unset", unset);
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
        if (this.bucketSetMap.get(bucketNum).containsKey(_id)) {
            if (!filter(_id, bucketNum, changeStreamEvent, "replace")) {
                return;
            }
        }
        bucketSetMap.get(bucketNum).put(_id, this.bucketWriteModelListMap.get(bucketNum).size());
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
        if (bucketSetMap.get(bucketNum).containsKey(_id)) {
            if (!filter(_id, bucketNum, changeStreamEvent, "delete")) {
                return;
            }
        }
        bucketSetMap.get(bucketNum).put(_id, this.bucketWriteModelListMap.get(bucketNum).size());

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


    public boolean filter(String _id, int bucketNum, ChangeStreamDocument<Document> changeStreamEvent, String newOperateType) {
        // 当发生了重复 则进入此状态处理
        // 获取重复的id信息是哪一条文档 操作
        Integer index = bucketSetMap.get(bucketNum).get(_id);

        WriteModel<Document> writeModel = this.bucketWriteModelListMap.get(bucketNum).get(index);
        String oldOperateType = "insert";
        if (writeModel instanceof InsertOneModel) {
            oldOperateType = "insert";
        } else if (writeModel instanceof DeleteOneModel) {
            oldOperateType = "delete";
        } else if (writeModel instanceof UpdateOneModel) {
            oldOperateType = "update";
        } else if (writeModel instanceof ReplaceOneModel) {
            oldOperateType = "replace";
        }


        log.info("_id:{},oldOperateType:{},newOperateType:{}", _id, oldOperateType, newOperateType);
//        {
//            if ("insert".equals(oldOperateType)) {
//                if ("insert".equals(newOperateType) || "update".equals(newOperateType)) {
//                    putDataToCache(currentDbTable, bucketNum);
//                    bucketSetMap.get(bucketNum).put(_id, 0);
//                    // true表示继续执行
//                    return true;
//                } else if ("delete".equals(newOperateType)) {
//                    // 只保留最后的删除即可
//                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
//                    this.bucketWriteModelListMap.get(bucketNum).add(index, new DeleteOneModel<Document>(changeStreamEvent.getDocumentKey()));
//                    return false;
//                } else if ("replace".equals(newOperateType)) {
//                    // replace 变成 delete and insert
//                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
//                    this.bucketWriteModelListMap.get(bucketNum).add(index, new DeleteOneModel<Document>(changeStreamEvent.getDocumentKey()));
//                    // 推到下一批次
//                    putDataToCache(currentDbTable, bucketNum);
//                    // 添加insert的语句
//                    bucketSetMap.get(bucketNum).put(_id, 0);
//                    this.bucketWriteModelListMap.get(bucketNum).add(new InsertOneModel<>(changeStreamEvent.getFullDocument()));
//                    // 不继续执行
//                    return false;
//                }
//            } else if ("update".equals(oldOperateType)) {
//                if ("insert".equals(newOperateType)) {
//                    putDataToCache(currentDbTable, bucketNum);
//                    bucketSetMap.get(bucketNum).put(_id, 0);
//                    // true表示继续执行
//                    return true;
//                } else if ("update".equals(newOperateType)) {
//                    // 变成一个replace
//                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
//                    this.bucketWriteModelListMap.get(bucketNum).add(index, new ReplaceOneModel<>(changeStreamEvent.getDocumentKey(), changeStreamEvent.getFullDocument(), new ReplaceOptions().upsert(true)));
//                    // true表示继续执行
//                    return false;
//                } else if ("delete".equals(newOperateType)) {
//                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
//                    this.bucketWriteModelListMap.get(bucketNum).add(index, new DeleteOneModel<Document>(changeStreamEvent.getDocumentKey()));
//                    // true表示继续执行
//                    return false;
//                } else if ("replace".equals(newOperateType)) {
//                    // ur =udi
//                    // 先删除
//                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
//                    this.bucketWriteModelListMap.get(bucketNum).add(index, new DeleteOneModel<Document>(changeStreamEvent.getDocumentKey()));
//                    // 推到下一批次
//                    putDataToCache(currentDbTable, bucketNum);
//                    // 添加insert的语句
//                    bucketSetMap.get(bucketNum).put(_id, 0);
//                    this.bucketWriteModelListMap.get(bucketNum).add(new InsertOneModel<>(changeStreamEvent.getFullDocument()));
//                    // true表示继续执行
//                    return false;
//                }
//            } else if ("delete".equals(oldOperateType)) {
//                if ("insert".equals(newOperateType)) {
//                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
//                    // this.bucketWriteModelListMap.get(bucketNum).add(index, new UpdateOneModel<Document>(changeStreamEvent.getDocumentKey(),new Document("$set",changeStreamEvent.getFullDocument())));
//                    this.bucketWriteModelListMap.get(bucketNum).add(index, new ReplaceOneModel<>(changeStreamEvent.getDocumentKey(), changeStreamEvent.getFullDocument()));
//                    // true表示继续执行
//                    return false;
//                }
//                // 推到下一批次后 继续执行
//                putDataToCache(currentDbTable, bucketNum);
//                bucketSetMap.get(bucketNum).put(_id, 0);
//                // true表示继续执行
//                return true;
//            } else if ("replace".equals(oldOperateType)) {
//                if ("insert".equals(newOperateType)) {
//                    // 推到下一批次后 继续执行
//                    putDataToCache(currentDbTable, bucketNum);
//                    bucketSetMap.get(bucketNum).put(_id, 0);
//                    // true表示继续执行
//                    return true;
//                }
//                if ("delete".equals(newOperateType)) {
//                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
//                    this.bucketWriteModelListMap.get(bucketNum).add(index, new DeleteOneModel<Document>(changeStreamEvent.getDocumentKey()));
//                    // true表示继续执行
//                    return false;
//                } else if ("update".equals(newOperateType)) {
//                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
//                    this.bucketWriteModelListMap.get(bucketNum).add(index, new ReplaceOneModel<>(changeStreamEvent.getDocumentKey(), changeStreamEvent.getFullDocument()));
//                } else if ("replace".equals(newOperateType)) {
//                    // ru =di
//                    // 先删除
//                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
//                    this.bucketWriteModelListMap.get(bucketNum).add(index, new DeleteOneModel<Document>(changeStreamEvent.getDocumentKey()));
//                    // 推到下一批次
//                    putDataToCache(currentDbTable, bucketNum);
//                    // 添加insert的语句
//                    bucketSetMap.get(bucketNum).put(_id, 0);
//                    this.bucketWriteModelListMap.get(bucketNum).add(new InsertOneModel<>(changeStreamEvent.getFullDocument()));
//                    // true表示继续执行
//                    return false;
//                }
//            }
//        }

        {
            if ("insert".equals(oldOperateType)) {
                if ("update".equals(newOperateType)) {
                    putDataToCache(currentDbTable, bucketNum);
                    bucketSetMap.get(bucketNum).put(_id, 0);
                    // true表示继续执行
                    return true;
                } else if ("replace".equals(newOperateType)) {
                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
                    this.bucketWriteModelListMap.get(bucketNum).add(index, new ReplaceOneModel<Document>(changeStreamEvent.getDocumentKey(), changeStreamEvent.getFullDocument(), new ReplaceOptions().upsert(true)));
                    return false;
                } else if ("delete".equals(newOperateType)) {
                    // 只保留最后的删除即可
                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
                    this.bucketWriteModelListMap.get(bucketNum).add(index, new DeleteOneModel<Document>(changeStreamEvent.getDocumentKey()));
                    return false;
                }
            } else if ("update".equals(oldOperateType)) {
                if ("update".equals(newOperateType)) {
                    putDataToCache(currentDbTable, bucketNum);
                    bucketSetMap.get(bucketNum).put(_id, 0);
                    // true表示继续执行
                    return true;
                } else if ("replace".equals(newOperateType)) {
                    // 变成一个replace
                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
                    this.bucketWriteModelListMap.get(bucketNum).add(index, new ReplaceOneModel<>(changeStreamEvent.getDocumentKey(), changeStreamEvent.getFullDocument(), new ReplaceOptions().upsert(true)));
                    // true表示继续执行
                    return false;
                } else if ("delete".equals(newOperateType)) {
                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
                    this.bucketWriteModelListMap.get(bucketNum).add(index, new DeleteOneModel<Document>(changeStreamEvent.getDocumentKey()));
                    // true表示继续执行
                    return false;
                }
            } else if ("delete".equals(oldOperateType)) {
                if ("insert".equals(newOperateType)) {
                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
                    this.bucketWriteModelListMap.get(bucketNum).add(index, new ReplaceOneModel<>(changeStreamEvent.getDocumentKey(), changeStreamEvent.getFullDocument(), new ReplaceOptions().upsert(true)));
                    // true表示继续执行
                    return false;
                }
            } else if ("replace".equals(oldOperateType)) {
                if ("delete".equals(newOperateType)) {
                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
                    this.bucketWriteModelListMap.get(bucketNum).add(index, new DeleteOneModel<Document>(changeStreamEvent.getDocumentKey()));
                    // true表示继续执行
                    return false;
                } else if ("update".equals(newOperateType) || "replace".equals(newOperateType)) {
                    this.bucketWriteModelListMap.get(bucketNum).remove(index);
                    this.bucketWriteModelListMap.get(bucketNum).add(index, new ReplaceOneModel<>(changeStreamEvent.getDocumentKey(), changeStreamEvent.getFullDocument(), new ReplaceOptions().upsert(true)));
                }
            }
        }
        return true;
    }

    public static void main(String[] args) {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }
        list.remove(50);
        list.add(50, 50);
        for (Integer sint : list) {
            System.out.println(sint);
        }
    }
}
