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
package com.whaleal.ddt.monitor.task;


import com.whaleal.ddt.monitor.service.ParseFileLogService;
import com.whaleal.ddt.monitor.service.WorkService;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.File;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * @author liheping
 */
@Configuration
@EnableScheduling
@Log4j2
public class ScheduledTask {

    @Autowired
    private WorkService taskService;

    @Autowired
    private ParseFileLogService parseFileLogService;

    private static final Object syncLock = new HashMap<>();

    private static volatile long filePointerTemp = 0L;

    private static AtomicBoolean isReadIng = new AtomicBoolean(false);

    @Value("${logPath}")
    private String logPath;


    private static volatile boolean isInit = false;

    private void init() {
        File file = new File(logPath+"/log/");
        if (file.exists() && file.isDirectory()) {
            for (File fileTemp : file.listFiles()) {
                if (fileTemp.isFile() && fileTemp.getName().endsWith("log.gz")) {
                    // 开始读取文件信息
                    log.info("开始读取文件:{}", fileTemp.getAbsolutePath());
                    parseFileLogService.readGZFile(fileTemp.getAbsolutePath());
                }
            }
        }
    }

    /**
     * 10s读取一次文件
     */
    @Scheduled(cron = "0/10 * * * * ? ")
    private void collectLog() {
        if (!isInit) {
            isReadIng.set(true);
            isInit = true;
            init();
            isReadIng.set(false);
        }

        // 如果第一次加摘 延迟1分钟进来
        Runnable runnable = () -> {
            if (!isReadIng.get() && isReadIng.compareAndSet(false, true)) {
                try {
                    File file = new File(logPath + "/log.log");
                    if (file.exists() && file.isFile()) {
                        long fileLength = file.length();
                        if (fileLength >= filePointerTemp) {
                            log.info("开始读取文件:{},startPointer:{},endPointer:{}", file.getAbsolutePath(), filePointerTemp, fileLength);
                            filePointerTemp = parseFileLogService.readFile(file.getAbsolutePath(), filePointerTemp, fileLength);
                        } else {
                            filePointerTemp = parseFileLogService.readFile(file.getAbsolutePath(), 0, fileLength);
                        }
                    }
                } finally {
                    isReadIng.set(false);
                }
            }
        };
        new Thread(runnable).start();
    }


}
