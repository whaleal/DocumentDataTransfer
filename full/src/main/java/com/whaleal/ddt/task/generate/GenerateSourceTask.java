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
package com.whaleal.ddt.task.generate;

import com.whaleal.ddt.task.CommonTask;
import lombok.extern.log4j.Log4j2;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 生成数据源任务类
 * 继承自通用任务类{@link CommonTask}
 */
@Log4j2
public class GenerateSourceTask extends CommonTask {

    /**
     * 是否生成数据源任务信息结束的标志，用于任务状态跟踪
     */
    private final AtomicBoolean isGenerateSourceTaskInfoOver;
    /**
     * 任务队列，用于存放数据源切分后的任务范围
     */
    private final BlockingQueue<Range> taskQueue;
    /**
     * 是否并行同步的标志，用于任务处理策略
     */

    private final boolean parallelSync;
    /**
     * 数据源队列，用于存放待切分的数据源
     */
    private final BlockingQueue<String> nsQueue;

    /**
     * 构造函数
     *
     * @param workName                     工作名称，用于任务标识
     * @param dsName                       数据源名称，用于任务执行的数据来源
     * @param isGenerateSourceTaskInfoOver 数据源任务信息是否生成完毕的标志
     * @param taskQueue                    任务队列，用于存放数据源切分后的任务范围
     * @param parallelSync                 是否并行同步的标志，用于任务处理策略
     * @param nsQueue                      数据源队列，用于存放待切分的数据ns
     */
    public GenerateSourceTask(String workName, String dsName,
                              AtomicBoolean isGenerateSourceTaskInfoOver,
                              BlockingQueue<Range> taskQueue,
                              boolean parallelSync,
                              BlockingQueue<String> nsQueue) {
        // 调用父类的构造函数，初始化工作名称和数据源名称
        super(workName, dsName);
        // 初始化其他成员变量
        this.isGenerateSourceTaskInfoOver = isGenerateSourceTaskInfoOver;
        this.taskQueue = taskQueue;
        this.parallelSync = parallelSync;
        this.nsQueue = nsQueue;
    }

    /**
     * 执行方法，用于处理数据源切分和任务生成
     */
    @Override
    public void execute() {
        // 循环处理待切分的数据源
        while (!nsQueue.isEmpty()) {
            // 从数据源队列中取出一个数据源
            String ns = nsQueue.poll();
            if (ns == null) {
                // 如果数据源为空，则跳出循环
                break;
            }
            // 输出日志，表示开始切分指定数据源的表数据
            log.info("{} 开始切分NS {}的表数据", workName, ns);
            // 创建 SpliceNsData 对象，并指定切分大小为32（可以根据实际情况配置）
            SpliceNsData spliceNsData = new SpliceNsData(dsName, 32);
            // 获取数据源切分后的任务范围列表
            List<Range> rangeList = spliceNsData.getRangeList(ns);
            // 输出日志，表示生成了多少个任务
            log.info("{} {}生成{}个任务", workName, ns, rangeList.size());
            // 遍历任务范围列表，并将每个任务放入任务队列中
            for (Range range : rangeList) {
                range.setNs(ns);
                try {
                    taskQueue.put(range);
                    // 目的为了乱序 多表并发
                    if (parallelSync && taskQueue.size() % 2 == 0) {
                        // 并行同步，每生成两个任务，就交换位置，用于实现任务乱序
                        Range oldRange = taskQueue.poll();
                        // 此队列为有限长度队列，否则当源端18w表，内存直接打爆
                        taskQueue.put(oldRange);
                    }
                } catch (InterruptedException e) {
                    // 处理中断异常，打印异常信息
                    e.printStackTrace();
                }
            }
        }
        // 数据源任务信息生成完毕，设置标志为true
        isGenerateSourceTaskInfoOver.set(true);
    }
}
