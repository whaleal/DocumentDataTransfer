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
package com.whaleal.ddt.sync.task.generate;

import com.whaleal.ddt.sync.task.SourceTaskInfo;
import com.whaleal.ddt.sync.task.read.FullSyncReadTask;
import com.whaleal.ddt.task.CommonTask;
import com.whaleal.ddt.thread.pool.ThreadPoolManager;
import lombok.extern.log4j.Log4j2;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 提交数据源任务类
 * 继承自通用任务类{@link CommonTask}
 * 用于将生成的数据源任务提交到线程池中执行
 */
@Log4j2
public class SubmitSourceTask extends CommonTask {
    /**
     * 最大读取线程数，用于控制并发执行的读取任务数量
     */
    private final int maxReadThreadNum;
    /**
     * 任务队列，用于存放数据源切分后的任务范围
     */
    private final BlockingQueue<Range> taskQueue;
    /**
     * 数据源任务信息是否生成完毕的标志
     */
    private final AtomicBoolean isGenerateSourceTaskInfoOver;
    /**
     * 源任务批处理大小
     */
    private final int batchSize;
    /**
     * 读取线程池名称，用于提交任务到指定线程池
     */
    private final String readThreadPoolName;


    /**
     * 构造函数
     *
     * @param workName                     工作名称，用于任务标识
     * @param dsName                       数据源名称，用于任务执行的数据来源
     * @param maxReadThreadNum             最大读取线程数，用于控制并发执行的读取任务数量
     * @param taskQueue                    任务队列，用于存放数据源切分后的任务范围
     * @param isGenerateSourceTaskInfoOver 数据源任务信息是否生成完毕的标志
     * @param batchSize                    源任务批处理大小
     * @param readThreadPoolName           读取线程池名称，用于提交任务到指定线程池
     */
    public SubmitSourceTask(String workName, String dsName, int maxReadThreadNum, BlockingQueue<Range> taskQueue, AtomicBoolean isGenerateSourceTaskInfoOver, int batchSize, String readThreadPoolName) {
        // 调用父类的构造函数，初始化工作名称和数据源名称
        super(workName, dsName);
        // 初始化其他成员变量
        this.maxReadThreadNum = maxReadThreadNum;
        this.taskQueue = taskQueue;
        this.isGenerateSourceTaskInfoOver = isGenerateSourceTaskInfoOver;
        this.batchSize = batchSize;
        this.readThreadPoolName = readThreadPoolName;
    }

    /**
     * 执行方法，用于提交数据源任务到线程池执行
     */
    @Override
    public void execute() {
        // 循环执行任务提交，直到满足退出条件
        while (true) {
            try {
                // 获取当前活跃的读取线程数量
                int activeThreadNum = ThreadPoolManager.getActiveThreadNum(readThreadPoolName);
                if (activeThreadNum >= maxReadThreadNum) {
                    // 如果活跃线程数达到最大值，则休眠一段时间继续检查
                    TimeUnit.SECONDS.sleep(1);
                    continue;
                }
                if (taskQueue.isEmpty() && isGenerateSourceTaskInfoOver.get()) {
                    // 如果任务队列为空且数据源任务信息已经生成完毕，则跳出循环，结束任务提交
                    break;
                }
                // 从任务队列中取出一个任务范围
                Range poll = taskQueue.poll();
                if (poll == null) {
                    // 如果取出的任务范围为空，则休眠一段时间继续检查
                    TimeUnit.SECONDS.sleep(1);
                    continue;
                }
                // 提交源任务到线程池中执行
                submitSourceTask(poll, batchSize);
            } catch (Exception ignored) {
                // 忽略异常
            }
        }
    }

    /**
     * 根据提供的范围和批处理大小提交源任务。
     *
     * @param range     源任务的范围。
     * @param batchSize 源任务的批处理大小。
     */
    public void submitSourceTask(Range range, int batchSize) {
        // 创建源任务信息对象，用于传递任务相关的信息
        SourceTaskInfo sourceTaskInfo = new SourceTaskInfo();
        sourceTaskInfo.setRange(range);
        sourceTaskInfo.setNs(range.getNs());
        sourceTaskInfo.setSourceDsName(dsName);
        sourceTaskInfo.setTargetDsName("");
        sourceTaskInfo.setStartTime(System.currentTimeMillis());
        sourceTaskInfo.setEndTime(0);
        // 输出日志，表示提交了一个任务
        log.info("{} submit task:{}", workName, sourceTaskInfo.toString());
        // 提交读取任务到线程池中执行，使用线程池管理器进行任务提交
        ThreadPoolManager.submit(readThreadPoolName, new FullSyncReadTask(workName, dsName, batchSize, sourceTaskInfo));
    }
}
