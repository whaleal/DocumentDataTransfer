
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
package com.whaleal.ddt.realtime.common.cache;


import com.mongodb.client.model.WriteModel;
import com.whaleal.ddt.cache.BatchDataEntity;
import lombok.Data;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

/**
 * MetaData类，用于存储和管理元数据操作日志。每个source只有一个event。
 * 通过静态方法获取和移除元数据操作日志对象。
 * 元数据操作日志主要用于记录event的相关信息，包括操作次数、队列信息等。
 *
 * @author liheping
 */
@ToString
@Log4j2
@Data
public final class RealTimeMetaData<T> {
    /**
     * workName 工作名称，用于标识元数据操作日志对象
     */
    private String workName;
    /**
     * ddl处理超时参数，默认值为1200秒
     */
    private int ddlWait = 1200;
    /**
     * 各种类型操作次数，使用Map存储不同类型操作的计器
     */
    private Map<String, LongAdder> bulkWriteInfo = new HashMap<>();

    /**
     * 方法用于更新批量写入信息
     *
     * @param type 类型
     * @param num  数量
     */
    public void updateBulkWriteInfo(String type, int num) {
        // 从map中获取与给定类型相对应的LongAdder
        LongAdder counter = bulkWriteInfo.get(type);
        // 将给定的数字加到计数器上
        counter.add(num);
    }

    // 初始化块，用于为不同类型创建LongAdder并将其放入bulkWriteInfo map中
    {
        // 创建一个新的LongAdder，并使用键"insert"将其放入map中
        bulkWriteInfo.put("insert", new LongAdder());
        // 创建一个新的LongAdder，并使用键"delete"将其放入map中
        bulkWriteInfo.put("delete", new LongAdder());
        // 创建一个新的LongAdder，并使用键"update"将其放入map中
        bulkWriteInfo.put("update", new LongAdder());
        // 创建一个新的LongAdder，并使用键"cmd"将其放入map中
        bulkWriteInfo.put("cmd", new LongAdder());
    }

    /**
     * 使用静态修饰符声明一个存储元数据操作日志的映射，
     * 使用字符串作为键，`RealTimeMetaData`作为值
     */
    private static Map<String, RealTimeMetaData> MetaDataMap = new ConcurrentHashMap<>();

    /**
     * 获取指定工作名称的元数据操作日志
     *
     * @param workName 工作名称
     * @return RealTimeMetaData
     */
    public static RealTimeMetaData getRealTimeMetaData(String workName) {
        return MetaDataMap.get(workName);
    }

    /**
     * 移除指定工作名称的元数据操作日志
     *
     * @param workName 工作名称
     */
    public static void removeRealTimeMetaData(String workName) {
        MetaDataMap.remove(workName);
    }

    /**
     * 构造函数，创建元数据操作日志对象
     *
     * @param workName            工作名称
     * @param ddlWait             ddl处理超时参数，默认值为1200秒
     * @param maxQueueSizeOfEvent event队列的最大大小
     * @param bucketNum           桶的数量
     * @param bucketSize          桶的大小
     */
    public RealTimeMetaData(String workName, int ddlWait, int maxQueueSizeOfEvent, int bucketNum, int bucketSize) {
        this.workName = workName;
        this.ddlWait = ddlWait;
        // 创建原始event数据的阻塞队列
        this.queueOfEvent = new LinkedBlockingQueue<>(maxQueueSizeOfEvent);
        for (int i = 0; i < bucketNum; i++) {
            // 为每个桶创建阻塞队列，并将其放入queueOfBucketMap中
            queueOfBucketMap.put(i, new LinkedBlockingQueue<>(bucketSize));
            // 为每个桶创建AtomicBoolean对象，默认值为false，并将其放入stateOfBucketMap中
            stateOfBucketMap.put(i, new AtomicBoolean(false));
        }
        // 将元数据操作日志对象放入MetaDataMap中，以工作名称为键，该对象为值
        MetaDataMap.put(workName, this);
    }

    /**
     * readNum 读取的event个数的计数器，使用LongAdder实现
     */
    private LongAdder readNum = new LongAdder();
    /**
     * 原始eventDocument数据的阻塞队列
     */
    private BlockingQueue<T> queueOfEvent;
    /**
     * 保存每个表的event的阻塞队列的映射
     * 键为表名，值为ns解析后的event
     */
    private final Map<String, BlockingQueue<T>> queueOfNsMap = new ConcurrentHashMap<>();
    /**
     * 保存每个表的状态的映射
     * 键为表名，值为原子类AtomicBoolean
     * 用来判断某表进行nsBucket时，是否被占用
     */
    private final Map<String, AtomicBoolean> stateOfNsMap = new ConcurrentHashMap<>();

    /**
     * 保存每个桶的批数据的阻塞队列的映射
     * 键为桶号，值为批数据的阻塞队列
     */
    private final Map<Integer, BlockingQueue<BatchDataEntity<WriteModel<Document>>>> queueOfBucketMap = new ConcurrentHashMap<>();
    /**
     * 保存每个桶的状态的映射
     * 键为桶号，值为原子类AtomicBoolean
     * 用来判断某桶，是否被占用
     */
    private final Map<Integer, AtomicBoolean> stateOfBucketMap = new ConcurrentHashMap<>();

    /**
     * 保存每个ns正在处理的event信息的映射
     * 键为ns名称，值为当前正在处理的event信息
     */
    private final Map<String, T> currentNsDealEventInfo = new ConcurrentHashMap<>();
    /**
     * 保存每个ns含有唯一索引个数的映射
     * 键为ns名称，值为唯一索引的个数
     */
    private final Map<String, Integer> uniqueIndexCollection = new ConcurrentHashMap<>();

    /**
     * 获取各种类型操作次数的总和
     *
     * @return 操作次数的总和
     */
    public long sumBulkWriteInfo() {
        return bulkWriteInfo.get("insert").sum() +
                bulkWriteInfo.get("delete").sum() +
                bulkWriteInfo.get("update").sum() +
                bulkWriteInfo.get("cmd").sum();
    }

    /**
     * 统计目前ns队列中缓存数据量
     *
     * @return 缓存数据量
     */
    public int cacheQueueOfNsDataNum() {
        int sum = 0;
        for (Map.Entry<String, BlockingQueue<T>> queueEntry : queueOfNsMap.entrySet()) {
            sum = +queueEntry.getValue().size();
        }
        return sum;
    }

    /**
     * 统计n个桶剩余数量
     *
     * @return 剩余数量
     */
    public int cacheBucketQueueDataNum() {
        int sum = 0;
        for (Map.Entry<Integer, BlockingQueue<BatchDataEntity<WriteModel<Document>>>> integerBlockingQueueEntry : queueOfBucketMap.entrySet()) {
            sum = +integerBlockingQueueEntry.getValue().size();
        }
        return sum;
    }

    /**
     * 获取所有缓存数据的总数量
     *
     * @return 缓存数据的总数量
     */
    public long getTotalCacheNum() {
        long sum = 0;
        // 队列缓存数
        sum += queueOfEvent.size();
        // NS队列缓存数
        sum += cacheQueueOfNsDataNum();
        // 桶缓存数
        sum += cacheBucketQueueDataNum();
        return sum;
    }

    /**
     * 等待缓存中数据执行完毕
     */
    public void waitCacheExe() {
        waitPushDataWriteOver();
        waitOplogNsBucketTask();
        waitPushDataWriteOver();
        waitOplogNsBucketTask();
    }

    /**
     * 在执行DDL之前等待数据写入完成
     */
    private void waitPushDataWriteOver() {
        long startWaitTime = System.currentTimeMillis();
        // 已经获取到的ns的csa锁的Set集合
        Set<Integer> bucketSet = new HashSet<>();
        while (true) {
            for (Map.Entry<Integer, BlockingQueue<BatchDataEntity<WriteModel<Document>>>> next : queueOfBucketMap.entrySet()) {
                // 桶号
                Integer bucketNum = next.getKey();
                final BlockingQueue<BatchDataEntity<WriteModel<Document>>> queue = next.getValue();
                AtomicBoolean atomicBoolean = stateOfBucketMap.get(bucketNum);
                boolean pre = atomicBoolean.get();
                // CAS操作
                if (!queue.isEmpty()) {
                    log.warn("bucket: {} remaining {} data has not been executed", bucketNum, queue.size());
                }
                if (queue.isEmpty() && (!pre) && atomicBoolean.compareAndSet(false, true)) {
                    // 已经获取到此ns的锁
                    bucketSet.add(bucketNum);
                    // 释放锁
                    atomicBoolean.set(false);
                }
            }
            // 获取dbTableBucketBatchDataQueueMap的key集合
            Set<Integer> keySet = new HashSet<>(queueOfBucketMap.keySet());
            // 默认是全部表都获得了锁
            boolean isAll = true;
            for (Integer bucketName : keySet) {
                if (!bucketSet.contains(bucketName)) {
                    isAll = false;
                    break;
                }
            }
            // 已经获取到了全部key的锁 && 缓存中没有数据了
            if (isAll && cacheBucketQueueDataNum() == 0) {
                break;
            }
            // 最多等待某个轮询操作600秒
            if (System.currentTimeMillis() - startWaitTime > 1000L * ddlWait) {
                log.error("{} an exception occurred while determining whether the bucket data has been parsed: Wait timeout {} seconds", workName, ddlWait);
                break;
            } else {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (Exception e) {
                    // 无需处理
                    log.error(e.getMessage());
                }
            }
        }
    }


    /**
     * 桶数据都已经写完
     */
    private void waitOplogNsBucketTask() {
        long startWaitTime = System.currentTimeMillis();
        // 已经获取到的ns的csa锁的Set集合
        Set<String> nsSet = new HashSet<>();
        while (true) {
            for (Map.Entry<String, BlockingQueue<T>> next : queueOfNsMap.entrySet()) {
                // 表名
                String dbTableName = next.getKey();
                BlockingQueue<T> queue = next.getValue();
                // 加'锁',每个数据表最多同时有一个线程解析
                AtomicBoolean atomicBoolean = stateOfNsMap.get(dbTableName);
                boolean pre = atomicBoolean.get();
                // CAS操作
                if (!queue.isEmpty()) {
                    log.warn("ns: {} remaining {} data has not been executed", dbTableName, queue.size());
                }
                if (queue.isEmpty() && !pre && atomicBoolean.compareAndSet(false, true)) {
                    // 已经获取到此ns的锁
                    nsSet.add(dbTableName);
                    // 释放锁
                    atomicBoolean.set(false);
                }
            }
            // 获取dbTableQueueOfNsMap的ns集合
            Set<String> keySet = new HashSet<>(queueOfNsMap.keySet());
            // 默认是全部表都获得了锁
            boolean isAll = true;
            for (String nsName : keySet) {
                if (!nsSet.contains(nsName)) {
                    isAll = false;
                    break;
                }
            }
            // 已经获取到了全部ns的锁 && 缓存中没有数据了
            if (isAll && cacheQueueOfNsDataNum() == 0) {
                break;
            }
            // 最多等待某个轮询操作600秒
            if (System.currentTimeMillis() - startWaitTime > 1000L * ddlWait) {
                log.error("{} an exception occurred while determining whether the ns data has been parsed: Wait timeout {} seconds", workName, ddlWait);
                break;
            } else {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (Exception e) {
                    // 无需处理
                    log.error(e.getMessage());
                }
            }
        }
    }


    /**
     * 输出缓存信息
     *
     * @param workStartTime   任务开始执行的时间
     * @param executeCountOld 上一次执行操作的次数
     * @return 执行的操作次数
     */
    public long printCacheInfo(long workStartTime, long lastPrintTime, long executeCountOld) {
        try {
            log.info("{} the total number of event read currently:{}", workName, readNum.sum());

            log.info("{} the current total number of caches:{}", workName, getTotalCacheNum());

            log.info("{} the current number of real-time synchronization caches:{}", workName, queueOfEvent.size());

            log.info("{} current bucket batch data cache number:{}", workName, cacheBucketQueueDataNum());

            log.info("{} current table data cache number:{}", workName, cacheQueueOfNsDataNum());

            log.info("{} current number of synchronization tables:{}", workName, getQueueOfNsMap().size());

            Document execute = new Document();
            for (Map.Entry<String, LongAdder> entry : bulkWriteInfo.entrySet()) {
                execute.append(entry.getKey(), entry.getValue().sum());
            }
            log.info("{} number of executions:{}", workName, execute.toJson());

            long exeCount = sumBulkWriteInfo();
            log.info("{} total number of execution items:{},average write speed:{} per/s",
                    workName, exeCount, exeCount / ((lastPrintTime - workStartTime) / 1000));

            log.info("{} current round (10s) execution:{} per/s", workName, Math.round((exeCount - executeCountOld) / 10));

            // 输出ns正在处理那个ddl oplog呢
            for (Map.Entry<String, T> documentEntry : currentNsDealEventInfo.entrySet()) {
                String key = documentEntry.getKey();
                // 输出ns正在处理那个ddl oplog呢
                log.info("{} ns:{},processing event:{}", workName, key, documentEntry.getValue().toString());
            }

            // 可以1分钟输出一次
            if (Calendar.getInstance().get(Calendar.SECOND) <= 10) {
                // 输出ns的缓存数量
                for (Map.Entry<String, BlockingQueue<T>> entry : queueOfNsMap.entrySet()) {
                    int size = entry.getValue().size();
                    if (size == 0) {
                        continue;
                    }
                    log.info("{} ns:{},remaining sync data:{}", workName, entry.getKey(), size);
                }
                // 输出桶的缓存数据
                for (Map.Entry<Integer, BlockingQueue<BatchDataEntity<WriteModel<Document>>>> entry : queueOfBucketMap.entrySet()) {
                    int size = entry.getValue().size();
                    if (size == 0) {
                        continue;
                    }
                    log.info("{} bucket:{},remaining sync data:{}", workName, entry.getKey(), size);
                }
            }
            return exeCount;
        } catch (Exception e) {
            log.error("{} error getting live program execution,msg:{}", workName, e.getMessage());
        }
        return 0L;
    }
}
