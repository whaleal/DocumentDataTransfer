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
package com.whaleal.ddt.sync.changestream.write;


import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.whaleal.ddt.cache.BatchDataEntity;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.sync.changestream.cache.MetadataEvent;
import com.whaleal.ddt.sync.connection.MongoDBConnectionSync;
import com.whaleal.ddt.task.CommonTask;
import com.whaleal.ddt.util.WriteModelUtil;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @desc: 写入数据
 * @author: lhp
 * @time: 2021/7/30 11:56 上午
 */
@Log4j2
public class RealTimeSyncWriteData extends CommonTask {

    /**
     * oplog元数据库类
     */
    private final MetadataEvent metadataEvent;
    /**
     * mongoClient
     */
    private final MongoClient mongoClient;


    private final int bucketSize;


    public RealTimeSyncWriteData(String workName, String dsName, int bucketSize) {
        super(workName, dsName);
        this.metadataEvent = MetadataEvent.getEventMetadata(workName);
        this.dsName = dsName;
        this.mongoClient = MongoDBConnectionSync.getMongoClient(dsName);
        this.workName = workName;
        this.bucketSize = bucketSize;
    }

    @Override
    public void execute() {
        log.warn("{} the oplog write data thread starts running", workName);
        int idlingTime = 0;
        while (true) {
            try {
                {
                    // 检查任务状态
                    if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_STOP) {
                        break;
                    }
                    if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_PAUSE) {
                        TimeUnit.SECONDS.sleep(5);
                    }
                }
                if (idlingTime++ > 10) {
                    // 10次都没有获取到oplog信息,则进行睡眠
                    TimeUnit.SECONDS.sleep(1);
                    // 10次都没有获得锁 更有可能继续无法获得'锁'
                    idlingTime = 9;
                }
                for (Map.Entry<Integer, BlockingQueue<BatchDataEntity>> next : metadataEvent.getQueueOfBucketMap().entrySet()) {
                    // 桶号
                    Integer bucketNum = next.getKey();
                    // 为空就不用进来处理了  也不用进行加锁信息
                    if (next.getValue().isEmpty()) {
                        idlingTime++;
                        continue;
                    }
                    AtomicBoolean atomicBoolean = metadataEvent.getStateOfBucketMap().get(bucketNum);
                    boolean pre = atomicBoolean.get();
                    if (!pre && atomicBoolean.compareAndSet(false, true)) {
                        try {
                            // 解析后的WriteModel队列
                            Queue<BatchDataEntity> documentQueue = next.getValue();
                            idlingTime = 0;
                            // 写入该表的数据
                            write(documentQueue, bucketNum);
                        } catch (Exception e) {
                            log.error("bucketNum:{},an error occurred while writing the oplog,msg:{}", bucketNum, e.getMessage());
                        } finally {
                            atomicBoolean.set(false);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("{} an error occurred while writing the oplog,msg:{}", workName, e.getMessage());
            }
        }

    }

    /**
     * write
     *
     * @param documentQueue 队列数据
     * @param bucketNum     桶号
     * @desc 执行写入
     */
    public void write(Queue<BatchDataEntity> documentQueue, int bucketNum) {
        int parseSize = 0;
        while (true) {
            BatchDataEntity batchDataEntity = documentQueue.poll();
            // batchDataEntity为null或已经写入了20批数据，则退出
            if (batchDataEntity == null) {
                break;
            }
            bulkExecute(batchDataEntity);
            // 有数据就一直写入
            // 一直有数据 就一直追加 此时大表中大幅度占有的时候 会阻塞其他线程的处理
            if (parseSize++ > bucketSize * 1024 * 10) {
                break;
            }
        }
    }

    /**
     * bulkExecute 批量写数据
     *
     * @param batchDataEntity 批数据
     * @desc 批量写数据
     */
    public void bulkExecute(BatchDataEntity batchDataEntity) {
        String dbTableName = batchDataEntity.getNs();
        List<WriteModel<Document>> list = batchDataEntity.getDataList();
        try {
            if (list.isEmpty()) {
                return;
            }
            String dbName = dbTableName.split("\\.", 2)[0];
            String tableName = dbTableName.split("\\.", 2)[1];
            final BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
            if (metadataEvent.getUniqueIndexCollection().containsKey(dbName + "." + tableName)) {
                bulkWriteOptions.ordered(true);
            }
            // q: 写入方式
            // a: 用户自己在url上配置
            BulkWriteResult bulkWriteResult = this.mongoClient.getDatabase(dbName).getCollection(tableName).
                    bulkWrite(list, bulkWriteOptions);
            bulkWriteInfo(bulkWriteResult);
        } catch (Exception e) {
            // 出现异常 就一条一条数据写入
            singleExecute(batchDataEntity);
        }
    }

    /**
     * singleExecute 批量写数据
     *
     * @param batchDataEntity 批数据
     * @desc 批量写数据
     */
    public void singleExecute(BatchDataEntity batchDataEntity) {
        String ns = batchDataEntity.getNs();
        List<WriteModel<Document>> list = batchDataEntity.getDataList();
        MongoNamespace mongoNamespace = new MongoNamespace(ns);
        for (WriteModel<Document> writeModel : list) {
            try {
                List<WriteModel<Document>> writeModelList = new ArrayList<>();
                writeModelList.add(writeModel);
                // q: 写入方式
                // a: 用户自己在url上配置
                BulkWriteResult bulkWriteResult = this.mongoClient.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName()).
                        bulkWrite(writeModelList, new BulkWriteOptions().ordered(true));
                bulkWriteInfo(bulkWriteResult);
            } catch (MongoBulkWriteException e) {
                for (BulkWriteError error : e.getWriteErrors()) {
                    log.error("ns:{},failed to write data:{}", ns, error.getMessage());
                }
                log.error("ns:{},write failed:{}", ns, WriteModelUtil.writeModelToString(writeModel));
            } catch (Exception e) {
                log.error("ns:{},failed to write data:{}", ns, e.getMessage());
                log.error("ns:{},write failed:{}", ns, WriteModelUtil.writeModelToString(writeModel));
            }
        }
    }

    /**
     * bulkWriteInfo 计算批数据写入情况
     *
     * @param bulkWriteResult 批数据写入情况
     * @desc 计算批数据写入情况
     */
    private void bulkWriteInfo(BulkWriteResult bulkWriteResult) {
        int insertedCount = bulkWriteResult.getInsertedCount();
        int deletedCount = bulkWriteResult.getDeletedCount();
        int modifiedCount = bulkWriteResult.getModifiedCount();
        metadataEvent.updateBulkWriteInfo("insert", insertedCount);
        metadataEvent.updateBulkWriteInfo("update", modifiedCount);
        metadataEvent.updateBulkWriteInfo("delete", deletedCount);
    }
}
