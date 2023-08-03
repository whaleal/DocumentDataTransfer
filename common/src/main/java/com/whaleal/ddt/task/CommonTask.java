/*
 * MongoT - An open-source project licensed under GPL+SSPL
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
package com.whaleal.ddt.task;


import com.whaleal.ddt.thread.pool.ThreadPoolManager;
import lombok.Data;
import lombok.extern.log4j.Log4j2;

import java.util.Calendar;

/**
 * @author liheping
 */
@Data
@Log4j2
public abstract class CommonTask implements Runnable {
    /**
     * 程序名
     */
    protected String workName;
    /**
     * 数据源名称
     */
    protected String dsName;

    protected CommonTask(String workName, String dsName) {
        this.workName = workName;
        this.dsName = dsName;
        printCopyRight();
    }

    protected CommonTask(String workName) {
        this.workName = workName;
    }

    public static void printCopyRight() {
        // 版本信息
        log.info(
                "\n" +
                        "This software is independently developed by Shanghai Jinmu Information Technology Co., Ltd. and unauthorized use is prohibited.\n" +
                        "For more information visit https://www.jinmuinfo.com .\n" +
                        "©2015-{} Shanghai Jinmu Information Technology Co., Ltd. All rights reserved ", Calendar.getInstance().get(Calendar.YEAR));
    }


    @Override
    public void run() {
        //获取线程名
        String[] split = Thread.currentThread().getName().split("_");
        StringBuilder threadPoolName = new StringBuilder();
        for (int i = 0; i < split.length - 1; i++) {
            threadPoolName.append(split[i]).append("_");
        }
        threadPoolName.deleteCharAt(threadPoolName.length() - 1);
        // 线程数+1
        ThreadPoolManager.updateActiveThreadNum(threadPoolName.toString(), 1);
        try {
            // 实际干活的入口
            execute();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("{} 线程:{}执行发生错误:{}", workName, Thread.currentThread().getName(), e.getMessage());
        }
        // 线程数-1
        ThreadPoolManager.updateActiveThreadNum(threadPoolName.toString(), -1);
    }

    /**
     * 线程实际执行入口
     */
    public abstract void execute();
}

