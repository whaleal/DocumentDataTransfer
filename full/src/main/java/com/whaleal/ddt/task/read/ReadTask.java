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
package com.whaleal.ddt.task.read;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.whaleal.ddt.cache.BatchDataEntity;
import com.whaleal.ddt.cache.MemoryCache;
import com.whaleal.ddt.connection.MongoDBConnection;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.task.CommonTask;
import com.whaleal.ddt.task.SourceTaskInfo;
import com.whaleal.ddt.task.generate.Range;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 读取数据任务类
 * 继承自通用任务类{@link CommonTask}
 * 用于从MongoDB数据库中读取数据并放入缓存中
 */
@Log4j2
public class ReadTask extends CommonTask {
    /**
     * mongoClient 客户端连接对象，用于与MongoDB进行数据交互
     */
    private final MongoClient mongoClient;
    /**
     * 缓存数据集合
     */
    private List<WriteModel<Document>> dataList = new ArrayList<>();
    /**
     * 是否读取完毕的标志
     */
    private boolean scanOver = false;
    /**
     * 每个批次数据的大小，默认为128
     */
    private int dataBatchSize = 128;
    /**
     * 任务配置信息
     */
    private SourceTaskInfo taskMetadata;
    /**
     * 读取条数
     */
    private int readNum = 0;
    /**
     * 当前缓存区中的条数
     */
    private int cacheTemp = 0;

    private final MemoryCache memoryCache;

    /**
     * 构造函数
     *
     * @param workName      工作名称，用于任务标识
     * @param dsName        数据源名称，用于获取对应的MongoDB连接
     * @param dataBatchSize 每个批次数据的大小，默认为128
     * @param taskMetadata  任务配置信息，包含数据源命名空间、开始和结束范围等信息
     */
    public ReadTask(String workName, String dsName, int dataBatchSize, SourceTaskInfo taskMetadata) {
        // 调用父类的构造函数，初始化工作名称和数据源名称
        super(workName, dsName);
        // 获取对应数据源的MongoDB连接客户端
        this.mongoClient = MongoDBConnection.getMongoClient(dsName);
        // 初始化其他成员变量
        this.taskMetadata = taskMetadata;
        this.dataBatchSize = dataBatchSize;
        this.memoryCache = MemoryCache.getMemoryCache(this.workName);
    }

    /**
     * 执行方法，用于从MongoDB数据库中读取数据并放入缓存中
     */
    @Override
    public void execute() {
        // 任务失败后可以继续向下执行
        while (!this.scanOver) {
            try {
                // 输出日志，标识开始执行源任务
                log.info("{} 开始执行源任务: {}", this.workName, this.taskMetadata.toString());
                if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_STOP) {
                    // 如果工作状态为停止，则跳出循环，结束任务执行
                    break;
                }
                // 设置taskMetadata的开始时间，后期会使用到该参数
                this.taskMetadata.setStartTime(System.currentTimeMillis());
                // 读取数据
                queryData();
            } catch (Exception e) {
                // 发生异常时，打印错误信息
                e.printStackTrace();
                log.error("{} 读取全量数据失败: {}", this.workName, e.getMessage());
            } finally {
                // 设置taskMetadata的结束时间，后期会使用到该参数
                this.taskMetadata.setEndTime(System.currentTimeMillis());
                this.taskMetadata.getRange().setRangeSize(this.readNum);
            }
        }
        // 计算任务执行时间
        long timeDiff = (this.taskMetadata.getEndTime() - this.taskMetadata.getStartTime());
        // 输出日志，标识源任务查询完成
        log.info("{} 源任务查询完成: {}, 使用时间 {} 毫秒, 读取 {} 条数据"
                , this.workName, this.taskMetadata.toString(), timeDiff, this.readNum);
    }

    /**
     * getDataFromCollection 获取表数据
     *
     * @desc 获取表数据
     */
    public void queryData() {
        this.dataList = new ArrayList<>();
        MongoNamespace mongoNamespace = new MongoNamespace(this.taskMetadata.getNs());
        Range range = this.taskMetadata.getRange();
        Object minValue = range.getMinValue();
        Object maxValue = range.getMaxValue();
        // 每读取一条，设置_id为minValueTemp
        Object minValueTemp = minValue;
        BasicDBObject condition = new BasicDBObject();
        condition.append("_id", new Document("$lt", maxValue).append("$gte", minValue));
        // 如果是range最大范围，则查询范围是[]。否则[)
        if (range.isMax()) {
            condition.append("_id", new Document("$lte", maxValue).append("$gte", minValue));
        }
        try {
            // 读取collection中的数据
            // todo 考虑$natural排序 bson读取
            MongoCursor<Document> mongoCursor = this.mongoClient.getDatabase(mongoNamespace.getDatabaseName()).getCollection(mongoNamespace.getCollectionName()).
                    find(condition).sort(new BasicDBObject().append("_id", 1)).iterator();
            while (mongoCursor.hasNext()) {
                this.readNum++;
                Document document = mongoCursor.next();
                this.dataList.add(new InsertOneModel<>(document));
                // 满一批数据时
                if (this.cacheTemp++ > this.dataBatchSize) {
                    // 把数据推送到缓存
                    putDataToCache();
                    // 简单的控制状态
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
                // 不需要进行校验是否为空之类的
                minValueTemp = document.get("_id");
            }
            this.scanOver = true;
        } catch (Exception e) {
            // 发生异常时，打印错误信息
            e.printStackTrace();
            log.error("{} 读取 [{}] 数据时发生错误, 错误信息: {}", this.workName, range.toString(), e.getMessage());
            Range rangeTemp = new Range();
            rangeTemp.setMinValue(minValueTemp);
            rangeTemp.setMaxValue(range.getMaxValue());
            rangeTemp.setMax(range.isMax());
            rangeTemp.setNs(this.taskMetadata.getNs());
            // 出现意外时，再次启动该任务实例
            this.taskMetadata = new SourceTaskInfo(rangeTemp, this.taskMetadata.getNs(), this.taskMetadata.getSourceDsName());
            this.scanOver = false;
        } finally {
            // 推送最后一批数据
            if (this.cacheTemp > 0) {
                putDataToCache();
                this.dataList = null;
            }
        }
    }

    /**
     * putDataToCache 推送数据到缓存区中
     *
     * @desc 推送数据到缓存区中
     */
    public void putDataToCache() {
        if (this.cacheTemp == 0) {
            return;
        }
        BatchDataEntity batchDataEntity = new BatchDataEntity();
        batchDataEntity.setDataList(this.dataList);
        batchDataEntity.setNs(this.taskMetadata.getNs());
        batchDataEntity.setSourceDsName(this.taskMetadata.getSourceDsName());
        batchDataEntity.setBatchNo(System.currentTimeMillis());
        // 推送数据到缓存区中
        this.memoryCache.putData(batchDataEntity);
        // 设置读取条数
        this.memoryCache.getReadDocCount().add(this.dataList.size());
        this.dataList = new ArrayList<>();
        this.cacheTemp = 0;
    }
}
