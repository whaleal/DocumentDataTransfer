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
