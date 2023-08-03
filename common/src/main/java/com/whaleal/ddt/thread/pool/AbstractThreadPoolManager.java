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
package com.whaleal.ddt.thread.pool;

import lombok.extern.log4j.Log4j2;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @description: 线程池的父类
 * @author: lhp
 * @time: 2021/8/25 2:01 下午
 */
@Log4j2
public class AbstractThreadPoolManager {
    /**
     * 核心线程数
     */
    protected int corePoolSize;
    /**
     * 线程次最大线程数
     */
    protected int maximumPoolSize;
    /**
     * 阻塞的线程数
     */
    protected int blockSize = Integer.MAX_VALUE;
    /**
     * 线程池
     */
    protected ExecutorService executorService;

    public AtomicInteger getActiveThreadNum() {
        return activeThreadNum;
    }

    private final AtomicInteger activeThreadNum = new AtomicInteger(0);

    private ThreadFactory initThreadFactory(String threadPoolName) {
        return new ThreadFactory() {
            // 线程所属地名称
            private final String threadFactoryName = threadPoolName;
            private final AtomicInteger threadId = new AtomicInteger(0);

            @Override
            public Thread newThread(Runnable r) {
                // 设置线程数+1
                return new Thread(r, threadFactoryName + "_" + threadId.addAndGet(1));
            }
        };
    }

    public AbstractThreadPoolManager(String threadPoolName, int corePoolSize, int maximumPoolSize, int blockSize) {
        this.corePoolSize = corePoolSize;
        this.maximumPoolSize = maximumPoolSize;
        this.blockSize = blockSize;
        ThreadFactory threadFactory = initThreadFactory(threadPoolName);
        executorService = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(this.blockSize), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
    }
}
