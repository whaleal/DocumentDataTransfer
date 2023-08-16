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
package com.whaleal.ddt.execute.full.common;

import com.whaleal.ddt.common.Datasource;
import com.whaleal.ddt.execute.config.WorkInfo;
import com.whaleal.ddt.execute.full.FullSync;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.sync.cache.MemoryCache;
import com.whaleal.ddt.sync.connection.MongoDBConnectionSync;
import com.whaleal.ddt.sync.metadata.MongoDBClusterManager;
import com.whaleal.ddt.sync.metadata.source.MongoDBMetadata;
import com.whaleal.ddt.sync.task.generate.GenerateSourceTask;
import com.whaleal.ddt.sync.task.generate.Range;
import com.whaleal.ddt.sync.task.generate.SubmitSourceTask;
import com.whaleal.ddt.sync.task.write.FullSyncWriteTask;
import com.whaleal.ddt.thread.pool.ThreadPoolManager;
import com.whaleal.ddt.util.HostInfoUtil;
import lombok.extern.log4j.Log4j2;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author liheping
 */
@Log4j2
public abstract class BaseFullWork {
    /**
     * workName
     */
    protected final String workName;
    /**
     * 数据源名称
     */
    protected final String sourceDsName;
    /**
     * 目标数据源名称
     */
    protected final String targetDsName;
    /**
     * 读取线程名称
     */
    protected final String readThreadPoolName;
    /**
     * 写入线程名称
     */
    protected final String writeThreadPoolName;
    /**
     * 公共线程名称
     */
    protected final String commonThreadPoolName;

    /**
     * FullSync类的构造函数。
     *
     * @param workName 工作名称。
     */
    protected BaseFullWork(String workName) {
        this.workName = workName;
        // 数据源名称
        this.sourceDsName = workName + "_source";
        this.targetDsName = workName + "_target";
        // 各种任务数据源名称
        this.readThreadPoolName = workName + "_readThreadPoolName";
        this.writeThreadPoolName = workName + "_writeThreadPoolName";
        this.commonThreadPoolName = workName + "_commonThreadPoolName";
    }

    /**
     * 使用提供的源数据库和目标数据库URL，读写线程数进行FullSync实例的初始化。
     *
     * @param sourceDsUrl    源数据库的URL。
     * @param targetDsUrl    目标数据库的URL。
     * @param readThreadNum  要使用的读线程数。
     * @param writeThreadNum 要使用的写线程数。
     */
    public void init(String sourceDsUrl, String targetDsUrl, int readThreadNum, int writeThreadNum) {
        // 建立连接 放在外部处理
        initConnection(sourceDsName, sourceDsUrl);
        initConnection(targetDsName, targetDsUrl);
        // 初始化线程次
        intiThreadPool(readThreadPoolName, readThreadNum);
        intiThreadPool(writeThreadPoolName, writeThreadNum);
        // 这个为系统自动生成的线程信息
        intiThreadPool(commonThreadPoolName, HostInfoUtil.computeTotalCpuCore() * 2);
    }

    /**
     * 根据提供的参数应用集群信息。
     *
     * @param clusterInfoSet     要应用的集群信息集合。
     * @param dbTableWhite       白名单数据库表，用于应用集群信息。
     * @param createIndexNum     要创建的索引数。
     * @param createIndexTimeOut 创建索引的超时时间。
     */
    public void applyClusterInfo(Set<String> clusterInfoSet, String dbTableWhite, int createIndexNum, long createIndexTimeOut) {
        MongoDBClusterManager mongoDBClusterManager = new MongoDBClusterManager(sourceDsName, targetDsName, workName, createIndexNum, createIndexTimeOut);
        mongoDBClusterManager.applyClusterInfo(clusterInfoSet, dbTableWhite);
    }

    /**
     * 根据提供的参数生成源任务信息。
     *
     * @param dbTableWhite                 白名单数据库表，用于生成源任务信息。
     * @param isGenerateSourceTaskInfoOver 原子布尔标志，指示源任务信息生成是否结束。
     * @param taskQueue                    用于存储生成的任务的阻塞队列。
     * @param parallelSync                 一个布尔值，指示是否启用并行同步。
     */
    public void generateSourceTaskInfo(String dbTableWhite, AtomicBoolean isGenerateSourceTaskInfoOver,
                                       BlockingQueue<Range> taskQueue, boolean parallelSync) {

        LinkedBlockingQueue<String> nsQueue = new LinkedBlockingQueue<>(new MongoDBMetadata(sourceDsName).getNSList(dbTableWhite));
        // 3个线程去切分 暂时3线程切分 性能尚可
        for (int i = 0; i < HostInfoUtil.computeTotalCpuCore() / 2; i++) {
            GenerateSourceTask generateSourceTask = new GenerateSourceTask(workName, sourceDsName,
                    isGenerateSourceTaskInfoOver, taskQueue, parallelSync,
                    nsQueue);
            // 都提交上common线程中
            createTask(commonThreadPoolName, generateSourceTask);
        }
    }

    /**
     * 创建任务
     *
     * @param threadPoolName 线程名
     * @param runnable       任务
     */
    private void createTask(String threadPoolName, Runnable runnable) {
        // 提交任何类型的任务
        ThreadPoolManager.submit(threadPoolName, runnable);
    }


    /**
     * 提交目标任务到目标数据库。
     *
     * @param writeThreadNum 用于提交目标任务的写线程数。
     */
    public void submitTargetTask(int writeThreadNum) {
        for (int i = 0; i < writeThreadNum; i++) {
            createTask(writeThreadPoolName, new FullSyncWriteTask(workName, targetDsName));
        }
    }

    /**
     * 根据提供的参数生成源任务。
     *
     * @param readThreadNum                要使用的读线程数。
     * @param taskQueue                    用于存储生成的任务的阻塞队列。
     * @param isGenerateSourceTaskInfoOver 原子布尔标志，指示源任务信息生成是否结束。
     * @param batchSize                    源任务的批处理大小。
     */
    public void generateSource(int readThreadNum, BlockingQueue<Range> taskQueue, AtomicBoolean isGenerateSourceTaskInfoOver, int batchSize) {
        SubmitSourceTask submitSourceTask = new SubmitSourceTask(workName, sourceDsName,
                readThreadNum, taskQueue, isGenerateSourceTaskInfoOver, batchSize, readThreadPoolName);
        createTask(commonThreadPoolName, submitSourceTask);
    }

    /**
     * 检查完整同步过程是否完成。
     *
     * @param memoryCache                  要检查的内存缓存。
     * @param isGenerateSourceTaskInfoOver 原子布尔标志，指示源任务信息生成是否结束。
     * @param taskQueue                    用于存储生成的任务的阻塞队列。
     * @return 如果完整同步完成，则返回true，否则返回false。
     */
    public boolean judgeFullSyncOver(MemoryCache memoryCache, AtomicBoolean isGenerateSourceTaskInfoOver, BlockingQueue<Range> taskQueue) {
        // isGenerateSourceTaskInfoOver=true
        // blockingQueue 队列为空
        // 活跃的读线程为0
        // 活跃的sys线程=0
        // 缓存中数据为0
        // q: 不够严谨 缺少拉取的数据 是否都已经写入进去
        // a: 增加睡眠1分钟。且 只有写入线程获取不到批数据，写入线程才退出。
        if (
                memoryCache.computeDocumentCount() == 0 &&
                        memoryCache.computeBatchCount() == 0 &&
                        isGenerateSourceTaskInfoOver.get() &&
                        // 这个要提前处理
                        taskQueue.isEmpty() &&
                        ThreadPoolManager.getActiveThreadNum(readThreadPoolName) == 0 &&
                        ThreadPoolManager.getActiveThreadNum(commonThreadPoolName) == 0
        ) {
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
     * 等待写入数据结束
     */
    public void waitWriteOver() {
        long startTime = System.currentTimeMillis();
        while (ThreadPoolManager.getActiveThreadNum(writeThreadPoolName) > 0) {
            try {
                TimeUnit.SECONDS.sleep(10);
                log.info("{} wait for the write thread to exit", workName);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // 等待1小时
            if (System.currentTimeMillis() - startTime > (3600 * 1000L)) {
                log.error("{} wait for the write thread to exit: The write thread is forced to exit after a timeout of one hour", workName);
                break;
            }
        }
    }

    /**
     * 使用给定的数据源名称和URL初始化连接。
     *
     * @param dsName 数据源的名称。
     * @param url    数据库的URL。
     */
    private void initConnection(String dsName, String url) {
        MongoDBConnectionSync.createMonoDBClient(dsName, new Datasource(url));
    }

    /**
     * 使用提供的线程池名称和核心池大小初始化线程池。
     *
     * @param threadPoolName 线程池的名称。
     * @param corePoolSize   核心池大小。
     */
    private void intiThreadPool(String threadPoolName, int corePoolSize) {
        ThreadPoolManager manager = new ThreadPoolManager(threadPoolName, corePoolSize, corePoolSize, Integer.MAX_VALUE);
    }

    /**
     * 打印每个线程池的当前活动线程数。
     */
    public void printThreadInfo() {
        log.info("{} the current number of {} threads:{}", workName, readThreadPoolName, ThreadPoolManager.getActiveThreadNum(readThreadPoolName));
        log.info("{} the current number of {} threads:{}", workName, writeThreadPoolName, ThreadPoolManager.getActiveThreadNum(writeThreadPoolName));
        log.info("{} the current number of {} threads:{}", workName, commonThreadPoolName, ThreadPoolManager.getActiveThreadNum(commonThreadPoolName));
    }

    /**
     * 销毁FullSync实例，清除内存缓存，关闭连接，并销毁线程池。
     *
     * @param memoryCache 要清除的内存缓存。
     */
    public void destroy(MemoryCache memoryCache) {
        // 清除gc
        memoryCache.gcMemoryCache();
        // 关闭连接池
        MongoDBConnectionSync.close(sourceDsName);
        MongoDBConnectionSync.close(targetDsName);
        // 关闭线程池
        ThreadPoolManager.destroy(readThreadPoolName);
        ThreadPoolManager.destroy(writeThreadPoolName);
        ThreadPoolManager.destroy(commonThreadPoolName);
    }

    /**
     * 计算与给定的数据库和表白名单匹配的所有命名空间中估计的文档总数。
     *
     * @param dbTableWhite 要考虑计算的数据库和表名白名单。
     * @return 所有匹配的命名空间中估计的文档总数。
     */
    public long estimatedAllNsDocumentCount(String dbTableWhite) {
        // 初始化变量以保存总计数
        long totalCount = 0L;
        // 使用提供的源数据源名称创建 MongoDBMetadata 对象
        MongoDBMetadata metadata = new MongoDBMetadata(sourceDsName);
        // 遍历基于提供的数据库和表白名单从元数据获取的命名空间列表
        for (String ns : metadata.getNSList(dbTableWhite)) {
            // 对于每个命名空间，将估计的文档计数添加到总计数中
            totalCount += metadata.estimatedDocumentCount(ns);
        }
        // 返回所有匹配的命名空间中估计的文档总数
        return totalCount;
    }

    /**
     * 启动全量同步任务
     *
     * @param workInfo 工作信息
     * @param fullType 全量类型
     */
    public static void startFullSync(final WorkInfo workInfo, final String fullType) {
        Runnable runnable = () -> {
            log.info("enable Start task :{}, task configuration information :{}", workInfo.getWorkName(), workInfo.toString());
            // 设置程序状态为运行中
            WorkStatus.updateWorkStatus(workInfo.getWorkName(), WorkStatus.WORK_RUN);
            // 生成缓存区数据
            MemoryCache memoryCache = new MemoryCache(workInfo.getWorkName(), workInfo.getBucketNum(), workInfo.getBucketSize());
            // 创建全量同步任务对象
            BaseFullWork fullSync = null;
            if ("sync".equals(fullType)) {
                fullSync = new FullSync(workInfo.getWorkName());
            } else {
                fullSync = new FullSync(workInfo.getWorkName());
            }
            // 开启任务执行，连接源数据库和目标数据库
            fullSync.init(workInfo.getSourceDsUrl(), workInfo.getTargetDsUrl(), workInfo.getSourceThreadNum(), workInfo.getTargetThreadNum());
            // 应用集群结构
            fullSync.applyClusterInfo(workInfo.getClusterInfoSet(), workInfo.getDbTableWhite(), workInfo.getCreateIndexThreadNum(), Integer.MAX_VALUE);
            // 生成sourceTaskInfo
            AtomicBoolean isGenerateSourceTaskInfoOver = new AtomicBoolean(false);
            // 默认缓存中128个读取任务
            BlockingQueue<Range> taskQueue = new LinkedBlockingQueue<>(128);
            fullSync.generateSourceTaskInfo(workInfo.getDbTableWhite(), isGenerateSourceTaskInfoOver, taskQueue, true);
            // 生成写入任务
            fullSync.submitTargetTask(workInfo.getTargetThreadNum());
            // 生成源数据库数据读取任务
            fullSync.generateSource(workInfo.getSourceThreadNum(), taskQueue, isGenerateSourceTaskInfoOver, workInfo.getBatchSize());
            // 计算一共要同步数据量
            long allNsDocumentCount = fullSync.estimatedAllNsDocumentCount(workInfo.getDbTableWhite());
            long writeCountOld = 0L;
            while (true) {
                try {
                    log.info("{} this full task is expected to transfer {} bars of data", workInfo.getWorkName(), allNsDocumentCount);
                    log.info("{} current task queue cache status:{}", workInfo.getWorkName(), taskQueue.size());
                    // 每隔10秒输出一次信息
                    TimeUnit.SECONDS.sleep(10);
                    // 输出缓存区运行情况
                    writeCountOld = memoryCache.printCacheInfo(workInfo.getStartTime(), writeCountOld);
                    // 输出任务各线程运行情况
                    fullSync.printThreadInfo();
                    // 输出缓存区中的信息
                    if (WorkStatus.getWorkStatus(workInfo.getWorkName()) == WorkStatus.WORK_STOP) {
                        break;
                    }
                    // 判断任务是否结束，如果结束则等待1分钟后退出循环
                    if (fullSync.judgeFullSyncOver(memoryCache, isGenerateSourceTaskInfoOver, taskQueue)) {
                        TimeUnit.MINUTES.sleep(1);
                        WorkStatus.updateWorkStatus(workInfo.getWorkName(), WorkStatus.WORK_STOP);
                        // 等待写入线程存活个数为0
                        fullSync.waitWriteOver();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            // 回收资源
            fullSync.destroy(memoryCache);
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
