package com.whaleal.ddt.reactive.read;/*
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


import com.mongodb.BasicDBObject;
import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.InsertOneModel;
import com.whaleal.ddt.common.generate.Range;
import com.whaleal.ddt.common.generate.SourceTaskInfo;
import com.whaleal.ddt.common.read.BaseFullReadTask;
import com.whaleal.ddt.conection.sync.MongoDBConnectionSync;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.task.CommonTask;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * 读取数据任务类
 * 继承自通用任务类{@link CommonTask}
 * 用于从MongoDB数据库中读取数据并放入缓存中
 * ******本质还是同步读取 不是异步读取******
 *
 * @author liheping
 */
@Log4j2
public class FullReactiveReadTask extends BaseFullReadTask {

    /**
     * mongoClient 客户端连接对象，用于与MongoDB进行数据交互
     */
    protected final MongoClient mongoClient;

    /**
     * 构造函数
     *
     * @param workName      工作名称，用于任务标识
     * @param dsName        数据源名称，用于获取对应的MongoDB连接
     * @param dataBatchSize 每个批次数据的大小，默认为128
     * @param taskMetadata  任务配置信息，包含数据源命名空间、开始和结束范围等信息
     */
    public FullReactiveReadTask(String workName, String dsName, int dataBatchSize, SourceTaskInfo taskMetadata) {
        // 调用父类的构造函数，初始化工作名称和数据源名称
        super(workName, dsName, dataBatchSize, taskMetadata);
        // 获取对应数据源的MongoDB连接客户端
        this.mongoClient = MongoDBConnectionSync.getMongoClient(dsName);
    }


    /**
     * getDataFromCollection 获取表数据
     *
     * @desc 获取表数据
     */
    @Override
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
            // Q: 考虑$natural排序 bson读取
            // A: $natural会加快查询速度。$natural排序排序后 就不能再次断点重传
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
            log.error("{} error message occurred while reading [{}] data:{}", this.workName, range.toString(), e.getMessage());
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
}
