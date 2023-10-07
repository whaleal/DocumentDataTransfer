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
package com.whaleal.ddt.common.write;

import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.whaleal.ddt.cache.BatchDataEntity;
import com.whaleal.ddt.common.cache.FullMetaData;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.task.CommonTask;
import lombok.extern.log4j.Log4j2;
import org.bson.BsonDocument;
import org.bson.Document;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * 写入数据任务类
 * 继承自通用任务类{@link CommonTask}
 * 用于将缓存中的批量数据写入到MongoDB数据库中
 * @author liheping
 */
@Log4j2
public abstract class BaseFullWriteTask extends CommonTask {

    protected static final BulkWriteOptions BULK_WRITE_OPTIONS = new BulkWriteOptions().ordered(true);
    /**
     * 构造函数
     *
     * @param workName 工作名称，用于任务标识
     * @param dsName   数据源名称，用于获取对应的MongoDB连接
     */
    protected BaseFullWriteTask(String workName, String dsName) {
        // 调用父类的构造函数，初始化工作名称和数据源名称
        super(workName, dsName);

    }

    /**
     * 执行方法，用于从缓存中获取数据并写入MongoDB数据库
     */
    @Override
    public void execute() {
        // 获取当前工作名称对应的内存缓存
        FullMetaData fullMetaData = FullMetaData.getFullMetaData(workName);
        // 循环执行数据写入任务
        while (true) {
            try {
                // 从缓存中获取一批数据
                BatchDataEntity<WriteModel<BsonDocument>> batchDataEntity = fullMetaData.getData();
                if (batchDataEntity != null) {
                    // todo test
                   // batchDataEntity.setNs(batchDataEntity.getNs()+"_ByDDT");
                    // 当前任务拉取的dbTableName
                    int successWriteNum = bulkExecute(batchDataEntity.getNs(), batchDataEntity.getDataList());
                    // 更新写入条数
                    fullMetaData.getWriteDocCount().add(successWriteNum);
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
     * 批量写入执行方法，用于处理批量写入的情况
     *
     * @param ns             数据库命名空间，用于指定数据库和集合
     * @param writeModelList 待写入的数据模型列表
     * @return 成功写入的数据条数
     */
    public abstract int bulkExecute(String ns, List<WriteModel<BsonDocument>> writeModelList);

}
