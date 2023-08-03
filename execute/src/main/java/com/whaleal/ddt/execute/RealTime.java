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

import com.whaleal.ddt.connection.Datasource;
import com.whaleal.ddt.connection.MongoDBConnection;
import com.whaleal.ddt.execute.config.WorkInfo;
import com.whaleal.ddt.parse.ns.ParseOplogNs;
import com.whaleal.ddt.parse.oplog.BucketOplog;
import com.whaleal.ddt.parse.oplog.BucketOplogForGteMongoDB5;
import com.whaleal.ddt.parse.oplog.BucketOplogForLtMongoDB5;
import com.whaleal.ddt.read.ReadOplog;
import com.whaleal.ddt.thread.pool.ThreadPoolManager;
import com.whaleal.ddt.write.WriteData;
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
public class RealTime {
    /**
     * 工作名称
     */
    private final String workName;
    /**
     * 源数据源名称
     */
    private final String sourceDsName;
    /**
     * 目标数据源名称
     */
    private final String targetDsName;
    /**
     * 读取Oplog的线程池名称
     */
    private final String readOplogThreadPoolName;
    /**
     * 解析NS的线程池名称
     */
    private final String parseNSThreadPoolName;
    /**
     * NS Bucket Oplog的线程池名称
     */
    private final String nsBucketOplogThreadPoolName;
    /**
     * 写入目标数据的线程池名称
     */
    private final String writeThreadPoolName;


    public RealTime(String workName) {
        this.workName = workName;
        // 数据源名称
        this.sourceDsName = workName + "_source";
        this.targetDsName = workName + "_target";
        // 各种任务数据源名称
        this.readOplogThreadPoolName = workName + "_readOplogThreadPoolName";
        this.parseNSThreadPoolName = workName + "_parseNSThreadPoolName";
        this.nsBucketOplogThreadPoolName = workName + "_nsBucketOplogThreadPoolName";
        this.writeThreadPoolName = workName + "_writeThreadPoolName";
    }

    /**
     * 初始化方法，建立连接到源数据源和目标数据源，并根据给定的总线程数计算出用于读取oplog、解析ns、分桶操作和写入的线程数量。
     * 然后创建相应的线程池。
     *
     * @param sourceDsUrl       源数据源的连接URL
     * @param targetDsUrl       目标数据源的连接URL
     * @param nsBucketThreadNum 分桶操作的线程数
     * @param writeThreadNum    写入目标数据的线程数
     */
    public void init(String sourceDsUrl, String targetDsUrl, int nsBucketThreadNum, int writeThreadNum) {
        // 建立连接 放在外部处理
        initConnection(sourceDsName, sourceDsUrl);
        initConnection(targetDsName, targetDsUrl);
        // 计算bucket 和 write部分的线程个数
        // 初始化线程次
        intiThreadPool(readOplogThreadPoolName, 1);
        intiThreadPool(parseNSThreadPoolName, 1);
        // 这个为系统自动生成的线程信息
        intiThreadPool(nsBucketOplogThreadPoolName, nsBucketThreadNum);
        intiThreadPool(writeThreadPoolName, writeThreadNum);
    }

    /**
     * 初始化MongoDB连接的方法，通过给定的数据源名称(dsName)和URL(url)创建MongoDB连接。
     *
     * @param dsName 数据源名称
     * @param url    数据源连接URL
     */
    private void initConnection(String dsName, String url) {
        MongoDBConnection.createMonoDBClient(dsName, new Datasource(url));
    }

    /**
     * 初始化线程池的方法，通过给定的线程池名称(threadPoolName)和核心线程数(corePoolSize)创建一个线程池。
     *
     * @param threadPoolName 线程池名称
     * @param corePoolSize   线程池的核心线程数
     */
    private void intiThreadPool(String threadPoolName, int corePoolSize) {
        ThreadPoolManager manager = new ThreadPoolManager(threadPoolName, corePoolSize, corePoolSize, Integer.MAX_VALUE);
    }

    /**
     * 创建任务并提交到指定的线程池。该方法接收线程池名称(threadPoolName)和要执行的任务(runnable)作为参数。
     *
     * @param threadPoolName 线程池名称
     * @param runnable       要执行的任务
     */
    private void createTask(String threadPoolName, Runnable runnable) {
        // 提交任何类型的任务
        ThreadPoolManager.submit(threadPoolName, runnable);
    }

    /**
     * 提交实时同步任务的方法。根据给定的任务信息(WorkInfo)和总线程数，创建相应数量的线程用于写入操作、分桶操作、解析ns和读取oplog。
     *
     * @param workInfo          实时同步任务的信息
     * @param nsBucketThreadNum 分桶操作的线程数
     * @param writeThreadNum    写入目标数据的线程数
     */
    public void submitTask(WorkInfo workInfo, int nsBucketThreadNum, int writeThreadNum) {

        //  写入线程
        for (int i = 0; i < writeThreadNum; i++) {
            WriteData writeData = new WriteData(workName, targetDsName, workInfo.getBucketSize());
            createTask(writeThreadPoolName, writeData);
        }
        // 分桶线程
        for (int i = 0; i < nsBucketThreadNum; i++) {
            createTask(nsBucketOplogThreadPoolName, generateOplogNsBucketTask(workInfo));
        }
        // 解析ns线程
        ParseOplogNs parseOplogNs = new ParseOplogNs(workName, workInfo.getDbTableWhite(),
                targetDsName, workInfo.getBatchSize() * workInfo.getBucketSize(), workInfo.getDdlFilterSet().contains("dropDatabase"));
        createTask(parseNSThreadPoolName, parseOplogNs);
        // 读取线程
        ReadOplog readOplog = new ReadOplog(workName, sourceDsName, !workInfo.getDdlFilterSet().isEmpty(), workInfo.getDbTableWhite(), workInfo.getStartOplogTime(), workInfo.getEndOplogTime(), workInfo.getDelayTime());
        createTask(readOplogThreadPoolName, readOplog);
    }

    /**
     * 生成用于分桶操作的任务。根据给定的任务信息(WorkInfo)和MongoDB版本，选择对应版本的BucketOplog实现并返回。
     *
     * @param workInfo 实时同步任务的信息
     * @return 分桶操作的任务实例
     */
    private BucketOplog generateOplogNsBucketTask(WorkInfo workInfo) {
        String version = MongoDBConnection.getVersion(sourceDsName);
        // 高版本 要对update的oplog特殊处理
        if (version.startsWith("5") || version.startsWith("6") || version.startsWith("7") || version.startsWith("8")) {
            return new BucketOplogForGteMongoDB5(workName, targetDsName, workInfo.getBucketNum(), workInfo.getClusterInfoSet(), workInfo.getDdlWait());
        } else {
            return new BucketOplogForLtMongoDB5(workName, targetDsName, workInfo.getBucketNum(), workInfo.getClusterInfoSet(), workInfo.getDdlWait());
        }
    }

    /**
     * 打印线程信息的方法。输出当前活动线程数量，包括读取oplog的线程、解析ns的线程、分桶操作的线程和写入的线程。
     */
    public void printThreadInfo() {
        String threadInfo = "{} the current number of {} threads:{}";
        log.info(threadInfo, workName, readOplogThreadPoolName, ThreadPoolManager.getActiveThreadNum(readOplogThreadPoolName));
        log.info(threadInfo, workName, parseNSThreadPoolName, ThreadPoolManager.getActiveThreadNum(parseNSThreadPoolName));
        log.info(threadInfo, workName, nsBucketOplogThreadPoolName, ThreadPoolManager.getActiveThreadNum(nsBucketOplogThreadPoolName));
        log.info(threadInfo, workName, writeThreadPoolName, ThreadPoolManager.getActiveThreadNum(writeThreadPoolName));
    }

    /**
     * 判断实时同步是否完成的方法。通过检查读取oplog的线程数是否为0，来判断是否完成实时同步。
     *
     * @return 如果实时同步已完成，返回true；否则返回false
     */
    public boolean judgeRealTimeSyncOver() {
        // 当不进行读取的时候 可以任务任务已经中断
        // todo 但是当读取完成后 未写入的数据怎么办？ 增量情况会缺少数据
        if (ThreadPoolManager.getActiveThreadNum(readOplogThreadPoolName) == 0) {
            try {
                TimeUnit.MINUTES.sleep(1);
                // 如果发现关机 则睡眠1分钟 使数据写入到磁盘里面
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return true;
        }
        return false;
    }

    /**
     * 销毁实时同步任务的方法。关闭MongoDB连接池和线程池，释放资源。
     */
    public void destroy() {
        // 清除gc
        //
        // 关闭连接池
        MongoDBConnection.close(sourceDsName);
        MongoDBConnection.close(targetDsName);
        // 关闭线程池
        ThreadPoolManager.destroy(readOplogThreadPoolName);
        ThreadPoolManager.destroy(parseNSThreadPoolName);
        ThreadPoolManager.destroy(nsBucketOplogThreadPoolName);
        ThreadPoolManager.destroy(writeThreadPoolName);
    }

}
