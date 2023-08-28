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
package com.whaleal.ddt.realtime.common.read;


import com.mongodb.client.MongoClient;
import com.whaleal.ddt.conection.sync.MongoDBConnectionSync;
import com.whaleal.ddt.realtime.common.cache.RealTimeMetaData;
import com.whaleal.ddt.task.CommonTask;
import org.bson.BsonTimestamp;

/**
 * 抽象基类，用于从MongoDB数据库中读取和处理实时数据。
 * 子类应实现source()方法来定义特定的数据读取逻辑。
 *
 * @param <T> 表示正在处理的数据类型的泛型参数。
 * @author liheping
 */
public abstract class BaseRealTimeReadData<T> extends CommonTask {
    /**
     * mongoClient
     */
    protected final MongoClient mongoClient;
    /**
     * 表过滤策略 使用正则表达式进行过滤
     */
    protected final String dbTableWhite;
    /**
     * 开始读取该数据源的时间
     */
    protected int startTimeOfOplog;
    /**
     * 延迟时间s
     */
    protected int delayTime = 0;
    /**
     * 结束读取该oplog的时间
     */
    protected final int endTimeOfOplog;
    /**
     * 是否同步DDL
     */
    protected final boolean captureDDL;
    /**
     * event元数据库类保存数据信息的地方
     */
    protected final RealTimeMetaData<T> metadata;
    /**
     * 是否读取完成
     */
    protected boolean isReadScanOver = false;
    /**
     * 数据源版本
     */
    protected final String dbVersion;

    /**
     * 最新oplog时间戳信息
     */
    protected BsonTimestamp lastOplogTs = new BsonTimestamp(0);

    protected int readBatchSize = 8096;

    /**
     * 构造函数，用于初始化基本实时数据读取器。
     *
     * @param workName         工作/任务的名称。
     * @param dsName           数据源的名称。
     * @param captureDDL       是否捕获DDL更改的标志。
     * @param dbTableWhite     用于表名称过滤的正则表达式。
     * @param startTimeOfOplog oplog数据读取的起始时间戳。
     * @param endTimeOfOplog   oplog数据读取的结束时间戳。
     * @param delayTime        数据处理的延迟时间（秒）。
     */
    protected BaseRealTimeReadData(String workName, String dsName, boolean captureDDL,
                                   String dbTableWhite, int startTimeOfOplog,
                                   int endTimeOfOplog, int delayTime,int readBatchSize) {
        super(workName, dsName);
        this.captureDDL = captureDDL;
        this.dbTableWhite = dbTableWhite;
        this.endTimeOfOplog = endTimeOfOplog;
        this.startTimeOfOplog = startTimeOfOplog;
        this.workName = workName;
        this.delayTime = delayTime;
        this.metadata = RealTimeMetaData.getRealTimeMetaData(workName);
        this.mongoClient = MongoDBConnectionSync.getMongoClient(dsName);
        this.dbVersion = MongoDBConnectionSync.getVersion(dsName);
        this.readBatchSize=readBatchSize;
    }

    /**
     * 子类需要实现的抽象方法。
     * 该方法应定义特定的数据读取逻辑。
     */
    public abstract void source();
}
