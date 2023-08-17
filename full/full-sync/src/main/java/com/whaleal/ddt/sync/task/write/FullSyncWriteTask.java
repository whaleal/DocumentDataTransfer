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
package com.whaleal.ddt.sync.task.write;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoNamespace;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.whaleal.ddt.cache.BatchDataEntity;

import com.whaleal.ddt.common.cache.MemoryCache;
import com.whaleal.ddt.sync.connection.MongoDBConnectionSync;
import com.whaleal.ddt.util.WriteModelUtil;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.task.CommonTask;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 写入数据任务类
 * 继承自通用任务类{@link CommonTask}
 * 用于将缓存中的批量数据写入到MongoDB数据库中
 */
@Log4j2
public class FullSyncWriteTask extends CommonTask {
    /**
     * mongoClient 客户端连接对象，用于与MongoDB进行数据交互
     */
    private final MongoClient mongoClient;
    /**
     * 构造函数
     *
     * @param workName 工作名称，用于任务标识
     * @param dsName   数据源名称，用于获取对应的MongoDB连接
     */
    public FullSyncWriteTask(String workName, String dsName) {
        // 调用父类的构造函数，初始化工作名称和数据源名称
        super(workName, dsName);
        // 获取对应数据源的MongoDB连接客户端
        this.mongoClient = MongoDBConnectionSync.getMongoClient(dsName);
    }

    /**
     * 执行方法，用于从缓存中获取数据并写入MongoDB数据库
     */
    @Override
    public void execute() {
        // 获取当前工作名称对应的内存缓存
        MemoryCache memoryCache = MemoryCache.getMemoryCache(workName);
        // 循环执行数据写入任务
        while (true) {
            try {
                // 从缓存中获取一批数据
                BatchDataEntity batchDataEntity = memoryCache.getData();
                if (batchDataEntity != null) {
                    // 当前任务拉取的dbTableName
                    int successWriteNum = bulkExecute(batchDataEntity.getNs(), batchDataEntity.getDataList());
                    // 更新写入条数
                    memoryCache.getWriteDocCount().add(successWriteNum);
                } else {
                    // 缓存中没有数据，则可以进行睡眠一段时间，等待数据生成
                    TimeUnit.SECONDS.sleep(1);
                    // 写入端不用进行判断pause，直接写入
                    if (WorkStatus.getWorkStatus(workName) == WorkStatus.WORK_STOP) {
                        // 如果工作状态为停止，则跳出循环，结束任务执行
                        break;
                    }
                }
            } catch (Exception e) {
                // 发生异常时，打印错误信息
                log.error("{} an error occurred while writing data. Error message:{}", workName, e.getMessage());
            }
        }
    }

    /**
     * 单条写入执行方法，用于处理写入失败的情况
     *
     * @param writeModelListOfParent 待写入的单条数据列表
     * @param ns                     数据库命名空间，用于指定数据库和集合
     * @return 成功写入的数据条数
     */
    public int singleExecute(List<WriteModel<Document>> writeModelListOfParent, String ns) {
        int successWriteNum = 0;
        MongoNamespace mongoNamespace = new MongoNamespace(ns);
        for (WriteModel<Document> writeModel : writeModelListOfParent) {
            try {
                // 重复一条一条尝试写入
                List<WriteModel<Document>> writeModelList = new ArrayList<>();
                writeModelList.add(writeModel);
                // 使用有序执行的BulkWriteOptions来尝试写入单条数据
                BulkWriteResult bulkWriteResult = this.mongoClient.getDatabase(mongoNamespace.getDatabaseName()).
                        getCollection(mongoNamespace.getCollectionName()).
                        bulkWrite(writeModelList, new BulkWriteOptions().ordered(true));

                successWriteNum += bulkWriteResult.getInsertedCount();
            } catch (MongoBulkWriteException e) {
                // 如果写入出现异常，打印错误信息
                for (BulkWriteError error : e.getWriteErrors()) {
                    log.error("ns:{},data write failure:{}", ns, error.getMessage());
                }
                log.error("ns:{},Ddata write failure:{}", ns, WriteModelUtil.writeModelToString(writeModel));
            } catch (Exception e) {
                // 如果写入出现其他异常，打印错误信息
                log.error("ns:{},data write failure:{}", ns, e.getMessage());
                log.error("ns:{},data write failure:{}", ns, WriteModelUtil.writeModelToString(writeModel));
            }
        }
        return successWriteNum;
    }

    /**
     * 批量写入执行方法，用于处理批量写入的情况
     *
     * @param ns             数据库命名空间，用于指定数据库和集合
     * @param writeModelList 待写入的数据模型列表
     * @return 成功写入的数据条数
     */
    public int bulkExecute(String ns, List<WriteModel<Document>> writeModelList) {
        int successWriteNum = 0;
        MongoNamespace mongoNamespace = new MongoNamespace(ns);
        try {
            if (writeModelList.isEmpty()) {
                // 如果待写入的数据模型列表为空，直接返回0
                return successWriteNum;
            }
            // 使用无序执行的BulkWriteOptions来尝试批量写入数据
            BulkWriteResult bulkWriteResult = this.mongoClient.getDatabase(mongoNamespace.getDatabaseName()).
                    getCollection(mongoNamespace.getCollectionName()).bulkWrite(writeModelList, new BulkWriteOptions().ordered(false));
            successWriteNum += bulkWriteResult.getInsertedCount();
        } catch (Exception e) {
            // 如果批量写入出现异常，打印异常信息，并尝试单条写入
            e.printStackTrace();
            successWriteNum += singleExecute(writeModelList, ns);
        }
        return successWriteNum;
    }
}
