package com.whaleal.ddt.execute;


import com.whaleal.ddt.cache.MetadataOplog;
import com.whaleal.ddt.cache.MemoryCache;
import com.whaleal.ddt.execute.config.Property;
import com.whaleal.ddt.execute.config.WorkInfo;
import com.whaleal.ddt.execute.config.WorkInfoGenerator;
import com.whaleal.ddt.metadata.util.HostInfoUtil;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.task.CommonTask;
import com.whaleal.ddt.task.generate.Range;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author liheping
 */
@Log4j2
public class Execute {


    static {
        log.info("D2T启动信息:hostName[{}],pid[{}],启动目录:[{}]", HostInfoUtil.getHostName(), HostInfoUtil.getProcessID(), HostInfoUtil.getProcessDir());
        log.info("JVM Info:{}", HostInfoUtil.getJvmArg());
        log.info("\n" +
                "  ____    ____    _____ \n" +
                " |  _ \\  |___ \\  |_   _|\n" +
                " | | | |   __) |   | |  \n" +
                " | |_| |  / __/    | |  \n" +
                " |____/  |_____|   |_|  \n" +
                "                        ");
        log.info("\nDocument Data Transfer - An open-source project licensed under GPL+SSPL\n" +
                "   \n" +
                "Copyright (C) [2023 - present ] [Whaleal]\n" +
                "   \n" +
                "This program is free software; you can redistribute it and/or modify it under the terms\n" +
                "of the GNU General Public License and Server Side Public License (SSPL) as published by\n" +
                "the Free Software Foundation; either version 2 of the License, or (at your option) any later version.\n" +
                "    \n" +
                "This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;\n" +
                "without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.\n" +
                "See the GNU General Public License and SSPL for more details.\n" +
                "  \n" +
                "For more information, visit the official website: [www.whaleal.com]");
        CommonTask.printCopyRight();
    }


    /**
     * 主程序入口
     *
     * @param args 启动参数
     */
    public static void main(String[] args) {
        // 设置配置文件的路径
        Property.setFileName("/Users/liheping/Desktop/project/DocumentDataTransfer/execute/src/main/resources/mongodbT.properties");

        // 检查是否传入了正确的启动参数
        if (args.length == 1) {
            // 如果只传入一个参数，则将该参数作为配置文件的路径
            if (args[0] == null || args[0].length() == 0) {
                // 输出错误信息
                log.error("请正确输入配置文件的路径");
                return;
            }
            Property.setFileName(args[0]);
        } else if (args.length == 2) {
            // 如果传入了两个参数，根据实际情况进行处理
            // (根据代码这里是空的，可能还有其他处理逻辑)
        } else {
            // 参数数量错误，输出错误信息
            log.info("启动参数错误");
        }

        // 生成工作信息
        WorkInfo workInfo = WorkInfoGenerator.generateWorkInfo();
        // 启动任务
        start(workInfo);
        // 退出程序
        System.exit(0);
    }

    /**
     * 启动任务的方法
     *
     * @param workInfo 工作信息
     */
    private static void start(WorkInfo workInfo) {
        // 获取工作名称
        String workName = workInfo.getWorkName();
        // 根据同步模式选择不同的启动方式
        if (workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL)) {
            // 全量同步模式
            startFullSync(workInfo);
        } else if (workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_REAL_TIME)) {
            // 实时同步模式
            startRealTime(workInfo);
        } else if (workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_INCREMENT)) {
            // 全量+增量同步模式
            // 先执行全量同步，然后再执行增量同步
            workInfo.setStartOplogTime((int) (System.currentTimeMillis() / 1000));
            workInfo.setWorkName(workName + "_full");
            startFullSync(workInfo);
            // 设置新的任务的时区
            workInfo.setEndOplogTime((int) (System.currentTimeMillis() / 1000));
            workInfo.setWorkName(workName + "_realTime");
            startRealTime(workInfo);
        } else if (workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_REAL_TIME)) {
            // 全量+实时同步模式
            // 先执行全量同步，然后再执行实时同步
            workInfo.setStartOplogTime((int) (System.currentTimeMillis() / 1000));
            workInfo.setWorkName(workName + "_full");
            startFullSync(workInfo);
            // 设置新的任务的时区
            workInfo.setWorkName(workName + "_realTime");
            startRealTime(workInfo);
        }
    }

    /**
     * 启动全量同步任务
     *
     * @param workInfo 工作信息
     */
    private static void startFullSync(WorkInfo workInfo) {
        // 设置程序状态为运行中
        WorkStatus.updateWorkStatus(workInfo.getWorkName(), WorkStatus.WORK_RUN);
        // 生成缓存区数据
        MemoryCache memoryCache = new MemoryCache(workInfo.getWorkName(), workInfo.getBucketNum(), workInfo.getBucketSize());
        // 创建全量同步任务对象
        FullSync fullSync = new FullSync(workInfo.getWorkName());
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
        long writeCountOld = 0L;
        while (true) {
            try {
                // 每隔10秒输出一次信息
                TimeUnit.SECONDS.sleep(10);
                // 输出缓存区中的信息
                if (WorkStatus.getWorkStatus(workInfo.getWorkName()) == WorkStatus.WORK_STOP) {
                    break;
                }
                // 输出缓存区运行情况
                writeCountOld = memoryCache.printCacheInfo(workInfo.getStartTime(), writeCountOld);
                // 输出任务各线程运行情况
                fullSync.printThreadInfo();
                // 判断任务是否结束，如果结束则等待1分钟后退出循环
                if (fullSync.judgeFullSyncOver(memoryCache, isGenerateSourceTaskInfoOver, taskQueue)) {
                    TimeUnit.MINUTES.sleep(1);
                    WorkStatus.updateWorkStatus(workInfo.getWorkName(), WorkStatus.WORK_STOP);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // 回收资源
        fullSync.destroy(memoryCache);
    }

    /**
     * 启动实时同步任务
     *
     * @param workInfo 工作信息
     */
    private static void startRealTime(WorkInfo workInfo) {
        // 设置程序状态为运行中
        WorkStatus.updateWorkStatus(workInfo.getWorkName(), WorkStatus.WORK_RUN);
        // 缓存区对线
        int maxQueueSizeOfOplog = workInfo.getBucketNum() * workInfo.getBucketSize() * workInfo.getBucketSize();
        MetadataOplog metadataOplog = new MetadataOplog(workInfo.getWorkName(), workInfo.getDdlWait(), maxQueueSizeOfOplog, workInfo.getBucketNum(), workInfo.getBucketSize());
        // 创建实时同步任务对象
        RealTime realTime = new RealTime(workInfo.getWorkName());
        // 初始化任务，连接源数据库和目标数据库
        realTime.init(workInfo.getSourceDsUrl(), workInfo.getTargetDsUrl(), workInfo.getRealTimeThreadNum());
        // 创建写入任务
        realTime.submitTask(workInfo, workInfo.getRealTimeThreadNum());
        long executeCountOld = 0L;
        while (true) {
            try {
                // 每隔10秒输出一次信息
                TimeUnit.SECONDS.sleep(10);
                // 输出线程运行情况
                realTime.printThreadInfo();
                // 输出缓存区运行情况
                executeCountOld = metadataOplog.printCacheInfo(workInfo.getStartTime(), executeCountOld);
                // 判断任务是否结束，如果结束则等待1分钟后退出循环
                if (realTime.judgeRealTimeSyncOver()) {
                    WorkStatus.updateWorkStatus(workInfo.getWorkName(), WorkStatus.WORK_STOP);
                    TimeUnit.MINUTES.sleep(1);
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // 回收资源
        realTime.destroy();
    }

}
