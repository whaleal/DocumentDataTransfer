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
package com.whaleal.ddt.execute;

import com.whaleal.ddt.execute.common.RealTimeWork;
import com.whaleal.ddt.execute.config.WorkInfo;
import com.whaleal.ddt.realtime.common.cache.MetaData;
import com.whaleal.ddt.status.WorkStatus;

import com.whaleal.ddt.sync.changestream.distribute.bucket.DistributeBucket;
import com.whaleal.ddt.sync.changestream.parse.ns.ParseNs;
import com.whaleal.ddt.sync.changestream.read.RealTimeReadDataByChangeStream;
import com.whaleal.ddt.sync.changestream.write.RealTimeWriteData;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.TimeUnit;

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
public class RealTimeChangeStream extends RealTimeWork {


    public RealTimeChangeStream(String workName) {
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
            RealTimeWriteData realTimeWriteOplogData = new RealTimeWriteData(workName, targetDsName, workInfo.getBucketSize());
            createTask(writeThreadPoolName, realTimeWriteOplogData);
        }
        // 分桶线程
        for (int i = 0; i < nsBucketThreadNum; i++) {
            createTask(nsBucketOplogThreadPoolName, new DistributeBucket(workName, targetDsName, workInfo.getBucketNum(), workInfo.getDdlFilterSet(), workInfo.getDdlWait()));
        }
        // 解析ns线程
        ParseNs distributeNs = new ParseNs(workName, workInfo.getDbTableWhite(),
                targetDsName, workInfo.getBatchSize() * workInfo.getBucketSize());

        createTask(parseNSThreadPoolName, distributeNs);
        // 读取线程
        RealTimeReadDataByChangeStream realTimeReadDataByChangeStream = new RealTimeReadDataByChangeStream(workName, sourceDsName, workInfo.getDdlFilterSet().size() > 0, workInfo.getDbTableWhite(), workInfo.getStartOplogTime(), workInfo.getEndOplogTime(), workInfo.getDelayTime());
        createTask(readOplogThreadPoolName, realTimeReadDataByChangeStream);
    }





    /**
     * 启动实时同步任务
     *
     * @param workInfo 工作信息
     */
    public static void startRealTimeChangeStream(final WorkInfo workInfo) {
        Runnable runnable = () -> {
            log.info("enable Start task :{}, task configuration information :{}", workInfo.getWorkName(), workInfo.toString());
            // 设置程序状态为运行中
            WorkStatus.updateWorkStatus(workInfo.getWorkName(), WorkStatus.WORK_RUN);
            // 缓存区对线
            int maxQueueSizeOfOplog = workInfo.getBucketNum() * workInfo.getBucketSize() * workInfo.getBucketSize();
            MetaData metadataOplog = new MetaData(workInfo.getWorkName(), workInfo.getDdlWait(), maxQueueSizeOfOplog, workInfo.getBucketNum(), workInfo.getBucketSize());
            // 创建实时同步任务对象
            RealTimeChangeStream realTimeOplog = new RealTimeChangeStream(workInfo.getWorkName());
            // 初始化任务，连接源数据库和目标数据库
            realTimeOplog.init(workInfo.getSourceDsUrl(), workInfo.getTargetDsUrl(), workInfo.getNsBucketThreadNum(), workInfo.getWriteThreadNum());
            // 创建写入任务
            realTimeOplog.submitTask(workInfo, workInfo.getNsBucketThreadNum(), workInfo.getWriteThreadNum());
            long executeCountOld = 0L;
            while (true) {
                try {
                    // 每隔10秒输出一次信息
                    TimeUnit.SECONDS.sleep(10);
                    // 输出线程运行情况
                    realTimeOplog.printThreadInfo();
                    // 输出缓存区运行情况
                    executeCountOld = metadataOplog.printCacheInfo(workInfo.getStartTime(), executeCountOld);
                    // 判断任务是否结束，如果结束则等待1分钟后退出循环
                    if (realTimeOplog.judgeRealTimeTaskFinish()) {
                        WorkStatus.updateWorkStatus(workInfo.getWorkName(), WorkStatus.WORK_STOP);
                        TimeUnit.MINUTES.sleep(1);
                        break;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // 回收资源
            realTimeOplog.destroy();
        };
        Thread thread = new Thread(runnable);
        thread.setName(workInfo.getWorkName() + "_execute");
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
