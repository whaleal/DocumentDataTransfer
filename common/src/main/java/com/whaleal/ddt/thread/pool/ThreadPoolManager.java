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

    /**
     * 操作某线程池使用数的个数
     *
     * @param threadPoolName 程序名称
     * @param num            线程数
     */
    public static int updateActiveThreadNum(String threadPoolName, int num) {
        if (!THREAD_POOL_MANAGER.containsKey(threadPoolName)) {
            log.warn("threadPoolName:{},更新线程数失败:线程池不存在", threadPoolName);
            return -1;
        }
        log.info("threadPoolName:{},updateActiveThreadNum:{}", threadPoolName, num);
        return THREAD_POOL_MANAGER.get(threadPoolName).getActiveThreadNum().addAndGet(num);
    }

    public static void setActiveThreadNum(String threadPoolName, int num) {
        log.info("threadPoolName:{},setActiveThreadNum:{}", threadPoolName, num);
        THREAD_POOL_MANAGER.get(threadPoolName).getActiveThreadNum().set(num);
    }

    public static int getActiveThreadNum(String threadPoolName) {
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



