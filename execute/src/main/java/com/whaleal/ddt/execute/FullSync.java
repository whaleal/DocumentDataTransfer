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

import com.whaleal.ddt.cache.MemoryCache;
import com.whaleal.ddt.connection.Datasource;
import com.whaleal.ddt.connection.MongoDBConnection;
import com.whaleal.ddt.metadata.source.MongoDBMetadata;
import com.whaleal.ddt.metadata.target.ApplyMongoDBMetadata;
import com.whaleal.ddt.util.HostInfoUtil;
import com.whaleal.ddt.task.generate.GenerateSourceTask;
import com.whaleal.ddt.task.generate.Range;
import com.whaleal.ddt.task.generate.SubmitSourceTask;

import com.whaleal.ddt.task.write.WriteTask;
import com.whaleal.ddt.thread.pool.ThreadPoolManager;
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

public class FullSync {
    /**
     * workName
     */
    private final String workName;
    /**
     * 数据源名称
     */
    private final String sourceDsName;
    /**
     * 目标数据源名称
     */
    private final String targetDsName;
    /**
     * 读取线程名称
     */
    private final String readThreadPoolName;
    /**
     * 写入线程名称
     */
    private final String writeThreadPoolName;
    /**
     * 公共线程名称
     */
    private final String commonThreadPoolName;

    /**
     * FullSync类的构造函数。
     *
     * @param workName 工作名称。
     */
    public FullSync(String workName) {
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
        MongoDBMetadata sourceMetadata = new MongoDBMetadata(sourceDsName);
        ApplyMongoDBMetadata applyMongoDBMetadata = new ApplyMongoDBMetadata(targetDsName, createIndexNum, createIndexTimeOut);
        // 可以改成 枚举状态 多枚举开始
        if (clusterInfoSet.contains("0")) {
            try {
                log.info("{} 开始删除目标端已经存在的表", workName);
                // 获取用户信息
                for (String ns : sourceMetadata.getNSList(dbTableWhite)) {
                    applyMongoDBMetadata.dropTable(ns);
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("{} 删除目标端已经存在的表,msg:{}", workName, e.getMessage());
            }
        }

        if (clusterInfoSet.contains("1")) {
            try {
                log.info("{} start outputting user information", workName);
                // 获取用户信息
                sourceMetadata.printUserInfo();
            } catch (Exception e) {
                e.printStackTrace();
                log.error("{} print out all user information in the shard,msg:{}", workName, e.getMessage());
            }
        }

        if (clusterInfoSet.contains("2")) {
            try {
                log.info("{} synchronize database table structures", workName);
                // 同步表结构
                applyMongoDBMetadata.createCollection(sourceMetadata.getCollectionOptionMap(dbTableWhite));
                applyMongoDBMetadata.createView(sourceMetadata.getViewOptionMap(dbTableWhite));
            } catch (Exception e) {
                e.printStackTrace();
                log.error("{} synchronize database table structures,msg:{}", workName, e.getMessage());
            }
        }
        // 先同步config.setting信息
        if (clusterInfoSet.contains("6")) {
            try {
                log.info("{} synchronize config.setting table", workName);
                // 同步config.setting表
                applyMongoDBMetadata.updateConfigSetting(sourceMetadata.getConfigSettingList());
            } catch (Exception e) {
                e.printStackTrace();
                log.error("{} synchronize config.setting table,msg:{}", workName, e.getMessage());
            }
        }

        if (clusterInfoSet.contains("3")) {
            try {
                log.info("{} synchronize database table index information", workName);
                // 同步索引信息
                applyMongoDBMetadata.createIndex(sourceMetadata.getIndexList(dbTableWhite));
            } catch (Exception e) {
                e.printStackTrace();
                log.error("{} synchronize database table index information,msg:{}", workName, e.getMessage());
            }
        }

        if (clusterInfoSet.contains("4")) {
            try {
                log.info("{} enable library sharding for all libraries", workName);
                // 开启库分片
                applyMongoDBMetadata.enableShardingDataBase(sourceMetadata.getShardingDBNameList());
            } catch (Exception e) {
                e.printStackTrace();
                log.error("{} enable library sharding for all libraries,msg:{}", workName, e.getMessage());
            }
        }

        if (clusterInfoSet.contains("5")) {
            try {
                log.info("{} synchronize database table shard keys", workName);
                // 同步shard key
                applyMongoDBMetadata.createShardKey(sourceMetadata.getShardKey(dbTableWhite));
            } catch (Exception e) {
                e.printStackTrace();
                log.error("{} synchronize database table shard keys,msg:{}", workName, e.getMessage());
            }
        }

        if (clusterInfoSet.contains("7")) {
            try {
                log.info("{} pre-splitting chunks of database tables", workName);
                // 预split表
                applyMongoDBMetadata.splitShardTable(sourceMetadata.getShardCollectionSplit(dbTableWhite));
            } catch (Exception e) {
                e.printStackTrace();
                log.error("{} pre-splitting chunks of database tables,msg:{}", workName, e.getMessage());
            }
        }
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
        for (int i = 0; i < 3; i++) {
            GenerateSourceTask generateSourceTask = new GenerateSourceTask(workName, sourceDsName,
                    isGenerateSourceTaskInfoOver, taskQueue, parallelSync,
                    nsQueue);
            // 都提交上common线程中
            createTask(commonThreadPoolName, generateSourceTask);
        }
    }

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
            createTask(writeThreadPoolName, new WriteTask(workName, targetDsName));
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
        // todo 不够严谨 缺少拉取的数据 是否都已经写入进去
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
     * 使用给定的数据源名称和URL初始化连接。
     *
     * @param dsName 数据源的名称。
     * @param url    数据库的URL。
     */
    private void initConnection(String dsName, String url) {
        MongoDBConnection.createMonoDBClient(dsName, new Datasource(url));
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
        MongoDBConnection.close(sourceDsName);
        MongoDBConnection.close(targetDsName);
        // 关闭线程池
        ThreadPoolManager.destroy(readThreadPoolName);
        ThreadPoolManager.destroy(writeThreadPoolName);
        ThreadPoolManager.destroy(commonThreadPoolName);
    }
}
