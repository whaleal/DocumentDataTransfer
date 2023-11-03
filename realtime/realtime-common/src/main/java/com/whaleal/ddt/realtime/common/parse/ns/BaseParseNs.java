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
package com.whaleal.ddt.realtime.common.parse.ns;

import com.mongodb.client.MongoClient;
import com.whaleal.ddt.conection.sync.MongoDBConnectionSync;
import com.whaleal.ddt.realtime.common.cache.RealTimeMetaData;
import com.whaleal.ddt.status.WorkStatus;
import com.whaleal.ddt.task.CommonTask;
import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * BaseParseNs类，作为解析命名空间的基类，提供了基本的解析操作，包括解析ns、添加更新索引信息、推送队列等。
 * 实现了CommonTask接口，需要子类实现具体的解析ns和添加更新索引信息方法。
 *
 * @author liheping
 */
@Log4j2
public abstract class BaseParseNs<T> extends CommonTask {

    /**
     * Event元数据库类
     */
    protected final RealTimeMetaData<T> metadata;
    /**
     * 表过滤策略 正则表达式处理,着重处理ddl操作表
     */
    protected final String dbTableWhite;
    /**
     * mongoClient
     */
    protected final MongoClient mongoClient;
    /**
     * 每个ns队列最大缓存个数
     */
    protected final int maxQueueSizeOfNs;

    /**
     * 当前count数 用于清除ns
     */
    private Long count = 0L;
    /**
     * 上次清除ns的时间
     */
    private Long lastCleanNsTime = System.currentTimeMillis();
    /**
     * 要同步的DDL列表
     */
    protected final Set<String> ddlSet;

    /**
     * 构造函数，初始化基本参数和MongoClient
     *
     * @param workName         工作名称
     * @param dbTableWhite     数据库表白名单
     * @param dsName           数据源名称
     * @param maxQueueSizeOfNs 每个ns队列的最大缓存数量
     */
    protected BaseParseNs(String workName, String dbTableWhite, String dsName, int maxQueueSizeOfNs, Set<String> ddlSet) {
        super(workName, dsName);
        this.dbTableWhite = dbTableWhite;
        this.workName = workName;
        this.metadata = RealTimeMetaData.getRealTimeMetaData(workName);
        this.mongoClient = MongoDBConnectionSync.getMongoClient(dsName);
        this.maxQueueSizeOfNs = maxQueueSizeOfNs;
        this.ddlSet = ddlSet;
    }


    /**
     * 主执行方法，包含任务状态判断、数据表解析、数据放入缓存等
     */
    public void exe() {
        while (true) {
            // 要加上异常处理 以防出现解析ns的线程异常退出
            T event = null;
            try {
                // 从原始的eventList进行解析，获取对应的ns
                event = metadata.getQueueOfEvent().poll();
                if (event != null) {
                    // 解析ns
                    parseNs(event);
                } else {
                    // 要是event太慢,count增加,lastCleanNsTime减少,此时也及时进行清除ns。
                    count += 1000;
                    lastCleanNsTime -= 1000;
                    // 代表event队列为空 暂时休眠
                    TimeUnit.SECONDS.sleep(1);
                    if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_STOP) {
                        break;
                    }
                    if (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_PAUSE) {
                        // 发生了限速就开始限制读取
                        while (WorkStatus.getWorkStatus(this.workName) == WorkStatus.WORK_PAUSE) {
                            TimeUnit.SECONDS.sleep(5);
                        }
                    }
                }
                cleanIdeLNs();
            } catch (Exception e) {
                if (event != null) {
                    log.error("{} currently parsing the event log:{}", workName, event.toString());
                }
                log.error("{} an error occurred in the split table thread of the event,msg:{}", workName, e.getMessage());
            }
        }
    }

    /**
     * 清除空闲的ns队列
     */
    private void cleanIdeLNs() {
        // 每100w条 && 10分钟 清除一下空闲ns表信息
        if (count++ > 1000000) {
            count = 0L;
            long currentTimeMillis = System.currentTimeMillis();
            // 10分钟
            if ((currentTimeMillis - lastCleanNsTime) > 1000 * 60 * 10L) {
                lastCleanNsTime = currentTimeMillis;
                log.warn("{} start removing redundant ns buckets", workName);
                for (Map.Entry<String, BlockingQueue<T>> queueEntry : metadata.getQueueOfNsMap().entrySet()) {
                    try {
                        BlockingQueue<T> value = queueEntry.getValue();
                        if (!value.isEmpty()) {
                            continue;
                        }
                        String key = queueEntry.getKey();
                        AtomicBoolean atomicBoolean = metadata.getStateOfNsMap().get(key);
                        boolean pre = atomicBoolean.get();
                        // cas操作
                        if (value.isEmpty() && !pre && atomicBoolean.compareAndSet(false, true)) {
                            metadata.getQueueOfNsMap().remove(key);
                            metadata.getStateOfNsMap().remove(key);
                            atomicBoolean.set(false);
                        }
                    } catch (Exception e) {
                        log.error("{} error in clearing free ns queue of event,msg:{}", workName, e.getMessage());
                    }
                }
            }
        }
    }

    /**
     * 抽象方法，需要子类实现具体的解析ns
     *
     * @param event 事件
     */
    public abstract void parseNs(T event) throws InterruptedException;

    /**
     * 抽象方法，需要子类实现具体的添加更新唯一索引信息
     *
     * @param ns 库名.表名
     */
    public abstract void addUpdateUniqueIndexInfo(String ns);

    /**
     * 将事件推送到队列
     *
     * @param ns    库名.表名
     * @param event 事件
     * @param isDDL 是否是DDL操作
     */
    public void pushQueue(String ns, T event, boolean isDDL) throws InterruptedException {
        // 多重DDL 保证DDL顺序性问题
        if (isDDL) {
            metadata.waitCacheExe();
        }
        if (!metadata.getQueueOfNsMap().containsKey(ns)) {
            metadata.getQueueOfNsMap().put(ns, new LinkedBlockingQueue<>(maxQueueSizeOfNs));
            metadata.getStateOfNsMap().put(ns, new AtomicBoolean());
            addUpdateUniqueIndexInfo(ns);
        }
        metadata.getQueueOfNsMap().get(ns).put(event);
        if (isDDL) {
            metadata.waitCacheExe();
        }
    }
}
