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
package com.whaleal.ddt.sync.execute.config;


import com.alibaba.fastjson2.JSON;
import com.whaleal.ddt.sync.connection.MongoDBConnection;
import com.whaleal.ddt.util.HostInfoUtil;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.bson.Document;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;


/**
 * @author: lhp
 * @time: 2021/7/22 2:00 下午
 * @desc: 配置文件类
 */
@Data
@NoArgsConstructor
public class WorkInfo implements Cloneable, Serializable {

    public static final String SYNC_MODE_ALL = "all";
    public static final String SYNC_MODE_REAL_TIME = "realTime";
    public static final String SYNC_MODE_ALL_AND_REAL_TIME = "allAndRealTime";
    public static final String SYNC_MODE_ALL_AND_INCREMENT = "allAndIncrement";


    /**
     * 任务名称
     */
    private String workName;
    /**
     * 源端数据源url
     */
    private String sourceDsUrl;
    /**
     * 目标数据源url
     */
    private String targetDsUrl;
    /**
     * 同步模式
     * 全量:all
     * 实时:realTime
     * 全量加增量:allAndRealTime
     */
    private String syncMode;
    /**
     * 表过滤正则
     */
    private String dbTableWhite;
    /**
     * 需同步的ddl集合
     * set中包含*号 则全部ddl都要同步
     */
    private Set<String> ddlFilterSet;

    /**
     * 全量同步时
     * sourceThreadNum 读取任务线程个数
     * targetThreadNum 写入任务线程个数
     * createIndexThreadNum 建立索引线程个数
     */
    private int sourceThreadNum = Math.round(HostInfoUtil.computeTotalCpuCore() * 0.25F);
    private int targetThreadNum = Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F);
    private int createIndexThreadNum = Math.round(HostInfoUtil.computeTotalCpuCore());

    /**
     * 实时同步时
     * 解析桶的线程数的线程数
     * 写入数据的线程数
     */
    private int nsBucketThreadNum = (int) Math.ceil((double) HostInfoUtil.computeTotalCpuCore() / 4.F);
    private int writeThreadNum = (int) Math.ceil((double) HostInfoUtil.computeTotalCpuCore() / 4.F) * 3;

    /**
     * ddl处理超时参数 单位秒 默认600s
     */
    private int ddlWait = 1200;

    /**
     * 每个缓存区缓存批次数量
     */
    private int bucketSize;
    /**
     * 缓存区个数
     */
    private int bucketNum;
    /**
     * 每批次数据的大小
     */
    private int batchSize;
    /**
     * 增量同步或实时同步时。时间戳格式，单位s
     * oplog的开始时间
     */
    private int startOplogTime = (int) (System.currentTimeMillis() / 1000);
    /**
     * 增量同步或实时同步时。时间戳格式，单位s
     * oplog的结束时间
     */
    private int endOplogTime = 0;
    /**
     * 延迟时间 单位秒
     */
    private int delayTime;
    /**
     * 程序启动时间
     */
    private volatile long startTime = System.currentTimeMillis();
    /**
     * 同步DDL信息
     */
    private Set<String> clusterInfoSet = new HashSet<>();

    public void setClusterInfoSet(Set<String> clusterInfoSet) {
        this.clusterInfoSet.addAll(clusterInfoSet);
    }

    @Override
    public String toString() {
        {
            // 打印工作配置信息 主要对url 账号密码加密处理
            Document document = Document.parse(JSON.toJSONString(this));
            document.append("sourceDsUrl", MongoDBConnection.printAndGetURLInfo(this.getWorkName(), this.getSourceDsUrl()));
            document.append("targetDsUrl", MongoDBConnection.printAndGetURLInfo(this.getWorkName(), this.getTargetDsUrl()));
            return document.toJson();
        }
    }
}
