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
package com.whaleal.ddt.thread.pool;

import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Log4j2
public final class ThreadPoolManager extends AbstractThreadPoolManager {
    /**
     * 线程池
     * k为 threadPoolName
     * v为线程池对象
     */
    private static final Map<String, ThreadPoolManager> THREAD_POOL_MANAGER = new ConcurrentHashMap<>();


    public ThreadPoolManager(String threadPoolName, int corePoolSize, int maximumPoolSize, int blockSize) {
        // threadPoolName
        super(threadPoolName, corePoolSize, maximumPoolSize, blockSize);
        THREAD_POOL_MANAGER.put(threadPoolName, this);
    }

    /**
     * 删除对象信息
     *
     * @param threadPoolName 程序名称
     */
    private static void deletePool(String threadPoolName) {
        THREAD_POOL_MANAGER.remove(threadPoolName);
        log.info("{} thread pool is closed", threadPoolName);
    }

    public static ThreadPoolManager getPool(String threadPoolName) {
        return THREAD_POOL_MANAGER.get(threadPoolName);
    }

    /**
     * 操作某线程池使用数的个数
     *
     * @param threadPoolName 程序名称
     * @param num            线程数
     */
    public static int updateActiveThreadNum(String threadPoolName, int num, boolean isPrint) {
        if (!THREAD_POOL_MANAGER.containsKey(threadPoolName)) {
            log.warn("threadPoolName:{},failed to update thread count: thread pool does not exist", threadPoolName);
            return -1;
        }
        if (isPrint) {
            log.info("threadPoolName:{},updateActiveThreadNum:{}", threadPoolName, num);
        }
        return THREAD_POOL_MANAGER.get(threadPoolName).getActiveThreadNum().addAndGet(num);
    }

    public static int updateActiveThreadNum(String threadPoolName, int num) {
        return updateActiveThreadNum(threadPoolName, num, false);
    }

    public static void setActiveThreadNum(String threadPoolName, int num) {
        log.info("threadPoolName:{},setActiveThreadNum:{}", threadPoolName, num);
        THREAD_POOL_MANAGER.get(threadPoolName).getActiveThreadNum().set(num);
    }

    public static int getActiveThreadNum(String threadPoolName) {
        if (!THREAD_POOL_MANAGER.containsKey(threadPoolName)) {
            return 0;
        }
        return THREAD_POOL_MANAGER.get(threadPoolName).getActiveThreadNum().get();
    }

    /**
     * submit 提交任务
     *
     * @param threadPoolName threadPoolName
     * @param runnable       runnable
     * @desc 提交任务
     */
    public static void submit(String threadPoolName, Runnable runnable) {
        try {
            THREAD_POOL_MANAGER.get(threadPoolName).executorService.submit(runnable);
        } catch (Exception e) {
            log.error("{} an error occurred while submitting the thread task:{}", threadPoolName, e.getMessage());
        }
    }

    /**
     * 销毁线程池
     *
     * @param threadPoolName threadPoolName
     * @desc 销毁线程池
     */
    public static void destroy(String threadPoolName) {
        try {
            THREAD_POOL_MANAGER.get(threadPoolName).executorService.shutdownNow();
            deletePool(threadPoolName);
        } catch (Exception e) {
            log.error("{} an error occurred while closing the thread pool:{}", threadPoolName, e.getMessage());
        }
    }
}



