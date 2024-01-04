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
package com.whaleal.ddt.execute.realtime;

import com.whaleal.ddt.conection.sync.MongoDBConnectionSync;
import com.whaleal.ddt.execute.config.WorkInfo;
import com.whaleal.ddt.execute.realtime.common.BaseRealTimeWork;
import com.whaleal.ddt.sync.distribute.bucket.DistributeBucket;
import com.whaleal.ddt.sync.distribute.bucket.DistributeBucketForGteMongoDB5;
import com.whaleal.ddt.sync.distribute.bucket.DistributeBucketForLtMongoDB5;
import com.whaleal.ddt.sync.parse.ns.ParseNs;
import com.whaleal.ddt.sync.read.RealTimeReadDataByOplog;
import com.whaleal.ddt.sync.write.RealTimeWriteData;
import lombok.extern.log4j.Log4j2;


/**
 * @desc: 实时同步
 * 主要用于MongoDB数据的实时同步
 * 包含连接建立、线程池初始化、任务提交、线程信息打印和资源销毁等功能
 * 实现了对oplog的实时读取、ns的解析、数据分桶和数据写入等操作
 * 可根据不同版本的MongoDB进行适配
 * 注意：7.0版本暂未适配，会使用默认的V2实现
 * @author: lhp
 * @time: 2021/11/15 10:46 上午
 */
@Log4j2
public class BaseRealTimeOplog extends BaseRealTimeWork {


    public BaseRealTimeOplog(String workName) {
        super(workName);
    }

    /**
     * 提交实时同步任务的方法。根据给定的任务信息(WorkInfo)和总线程数，创建相应数量的线程用于写入操作、分桶操作、解析ns和读取oplog。
     *
     * @param workInfo          实时同步任务的信息
     * @param nsBucketThreadNum 分桶操作的线程数
     * @param writeThreadNum    写入目标数据的线程数
     */
    @Override
    public void submitTask(WorkInfo workInfo, int nsBucketThreadNum, int writeThreadNum) {
        //  写入线程
        for (int i = 0; i < writeThreadNum; i++) {
            RealTimeWriteData realTimeWriteData = new RealTimeWriteData(workName, targetDsName, workInfo.getBucketSize());
            createTask(writeThreadPoolName, realTimeWriteData);
        }
        // 分桶线程
        for (int i = 0; i < nsBucketThreadNum; i++) {
            createTask(nsBucketEventThreadPoolName, generateOplogNsBucketTask(workInfo));
        }
        // 解析ns线程
        ParseNs parseNs = new ParseNs(workName, workInfo.getDbTableWhite(),
                targetDsName, workInfo.getBatchSize() * workInfo.getBucketSize(), workInfo.getDdlFilterSet());

        createTask(parseNSThreadPoolName, parseNs);

        // 读取线程
        RealTimeReadDataByOplog realTimeReadDataByOplog = new RealTimeReadDataByOplog(workName, sourceDsName, workInfo.getDdlFilterSet().size() > 0, workInfo.getDbTableWhite(),
                workInfo.getStartOplogTime(), workInfo.getEndOplogTime(), workInfo.getDelayTime(),
                workInfo.getBucketNum() * workInfo.getBucketSize() * workInfo.getBucketSize()
                , workInfo.getOplogNS(),workInfo.getWapURL());
        createTask(readEventThreadPoolName, realTimeReadDataByOplog);
    }

    /**
     * 生成用于分桶操作的任务。根据给定的任务信息(WorkInfo)和MongoDB版本，选择对应版本的BucketOplog实现并返回。
     *
     * @param workInfo 实时同步任务的信息
     * @return 分桶操作的任务实例
     */
    private DistributeBucket generateOplogNsBucketTask(WorkInfo workInfo) {
        String version = MongoDBConnectionSync.getVersion(sourceDsName);
        version="5.";
        // 高版本 要对update的oplog特殊处理
        if (version.startsWith("5") || version.startsWith("6") || version.startsWith("7") || version.startsWith("8")) {
            return new DistributeBucketForGteMongoDB5(workName, sourceDsName, targetDsName, workInfo.getBucketNum(), workInfo.getDdlFilterSet(), workInfo.getDdlWait());
        } else {
            return new DistributeBucketForLtMongoDB5(workName, sourceDsName, targetDsName, workInfo.getBucketNum(), workInfo.getDdlFilterSet(), workInfo.getDdlWait());
        }
    }


}
