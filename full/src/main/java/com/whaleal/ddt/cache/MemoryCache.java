package com.whaleal.ddt.cache;


import lombok.Data;
import lombok.extern.log4j.Log4j2;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * @author: lhp
 * @time: 2021/7/20 9:53 上午
 * @desc: 数据缓存类
 */
@Log4j2
@Data
public class MemoryCache {
    /**
     * 任务名
     */
    private String workName;
    /**
     * 缓存桶数量
     * 默认20个
     */
    private int partitionNum = 20;
    /**
     * 每个缓存桶缓存个数
     * 默认20个
     */
    private int partitionSize = 20;
    /**
     * 缓存队列 用户缓存数据使用
     */
    private final Map<Integer, BlockingQueue<BatchDataEntity>> partitionQueueMap;
    /**
     * 某缓存区是否被使用
     */
    private final Map<Integer, AtomicBoolean> partitionStateMap;
    /**
     * 一共读取条数
     */
    private final LongAdder readDocCount = new LongAdder();
    /**
     * 一共写入条数
     */
    private final LongAdder writeDocCount = new LongAdder();

    /**
     * 存储每个工作名称对应的内存缓存的映射。
     */
    private static Map<String, MemoryCache> memoryCacheMap = new ConcurrentHashMap<>();

    /**
     * 获取指定工作名称对应的内存缓存。
     *
     * @param workName 工作名称。
     * @return 内存缓存对象。
     */
    public static MemoryCache getMemoryCache(String workName) {
        return memoryCacheMap.get(workName);
    }

    /**
     * 移除指定工作名称对应的内存缓存。
     * 调用内存缓存对象的gcMemoryCache方法进行垃圾回收，然后从映射中移除。
     *
     * @param workName 工作名称。
     */
    public static void removeMemoryCache(String workName) {
        MemoryCache memoryCache = memoryCacheMap.get(workName);
        if (memoryCache == null) {
            return;
        }
        memoryCacheMap.get(workName).gcMemoryCache();
        memoryCacheMap.remove(workName);
    }

    /**
     * init
     *
     * @desc 初始化缓存区类
     */
    public MemoryCache(String workName, int partitionNum, int partitionSize) {
        this.workName = workName;
        this.partitionSize = partitionSize;
        this.partitionNum = partitionNum;
        partitionQueueMap = new ConcurrentHashMap<>();
        partitionStateMap = new ConcurrentHashMap<>();
        for (int i = 0; i < partitionNum; i++) {
            partitionQueueMap.put(i, new LinkedBlockingQueue<>(partitionSize));
            partitionStateMap.put(i, new AtomicBoolean(false));
        }
        // 塞入到static中
        memoryCacheMap.put(workName, this);
    }

    /**
     * getData
     *
     * @return BatchDataEntity
     * @desc 塞入数据
     */
    public BatchDataEntity getData() {
        // 返回的数据
        BatchDataEntity returnValue = null;
        // 没有获取对缓存区的次数。即空跑次数
        // todo 空跑次数 可以来用做动态平衡读写
        int idlingTime = 0;
        while (idlingTime++ < partitionNum * 4) {
            // 随机数 范围[0,bucketNum)
            int partition = (int) ((Math.random() * 100) % partitionNum);
            // CAS操作
            boolean pre = partitionStateMap.get(partition).get();
            // CAS操作获取缓存区使用权限
            if (!pre && partitionStateMap.get(partition).compareAndSet(false, true)) {
                if (!partitionQueueMap.get(partition).isEmpty()) {
                    // 设置返回值
                    returnValue = partitionQueueMap.get(partition).poll();
                    idlingTime = partitionNum * 8;
                }
                // 释放'锁'
                partitionStateMap.get(partition).set(false);
            }
        }
        return returnValue;
    }

    /**
     * putData
     *
     * @param data
     * @desc 塞入数据
     */
    public void putData(BatchDataEntity data) {
        // 没有获取对缓存区的次数。即空跑次数
        // todo 空跑次数 可以来用做动态平衡读写
        int idlingTime = 0;
        while (true) {
            idlingTime++;
            // 随机数 范围[0,bucketNum)
            int partition = (int) ((Math.random() * 100) % partitionNum);
            boolean pre = partitionStateMap.get(partition).get();
            // CAS操作获取缓存区使用权限
            if (!pre && partitionStateMap.get(partition).compareAndSet(false, true)) {
                // 缓存区是否已满，未满则塞入数据
                if (partitionQueueMap.get(partition).size() < partitionSize) {
                    try {
                        partitionQueueMap.get(partition).put(data);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    partitionStateMap.get(partition).set(false);
                    break;
                }
                // 释放'锁'
                partitionStateMap.get(partition).set(false);
            }
            if (idlingTime++ < partitionNum * 4) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    /**
     * getAllDataBucketNum
     *
     * @desc 获取所有的缓存桶批数据个数
     */
    public long computeBatchCount() {
        // 非原子性操作
        long sum = 0;
        for (int i = 0; i < partitionNum; i++) {
            sum += partitionQueueMap.get(i).size();
        }
        return sum;
    }

    public long computeDocumentCount() {
        // 非原子性操作
        long sum = 0;
        for (int i = 0; i < partitionNum; i++) {
            BlockingQueue<BatchDataEntity> batchDataEntities = partitionQueueMap.get(i);
            for (BatchDataEntity dataEntity : batchDataEntities) {
                sum += dataEntity.getDataList().size();
            }
        }
        return sum;
    }

    /**
     * gcMemoryCache
     *
     * @desc 释放所有缓存数组
     */
    public void gcMemoryCache() {
        for (int i = 0; i < partitionNum; i++) {
            partitionQueueMap.get(i).clear();
            partitionQueueMap.get(i).clear();
        }
        partitionStateMap.clear();
        partitionQueueMap.clear();
    }

    public long printCacheInfo(long workStartTime, long writeCountOld) {
        try {
            // 当前时间
            Date currentTime = new Date();
            int diffTime = (int) ((currentTime.getTime() - workStartTime) / 1000);
            // 当前缓存区批数量
            log.info("{} number of batches remaining in the current buffer:{}", workName, computeBatchCount());
            // 当前缓存区条数量
            log.info("{} number of documents remaining in the com.whaleal.ddt.cache:{}", workName, computeDocumentCount());
            // 已写入的条数
            long writeCount = writeDocCount.sum();
            log.info("{} number of bars written:{},time cost:{}s,average com.whaleal.ddt.write speed:{} per/s",
                    workName, writeCount, diffTime, (writeCount / diffTime));
            // 已读取的条数
            long readCount = readDocCount.sum();
            log.info("{} number of bars com.whaleal.ddt.read:{},time cost:{}s,average com.whaleal.ddt.write speed:{} per/s",
                    workName, readCount, diffTime, (readCount / diffTime));
            log.info("{} the average com.whaleal.ddt.write speed of this round (10s):{} per/s", workName, Math.round((writeCount - writeCountOld) / 10));
            return writeCount;
        } catch (Exception e) {
            log.error("{} error getting full program execution,msg:{}", workName, e.getMessage());
        }
        return 0L;
    }
}

