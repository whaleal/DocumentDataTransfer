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
package com.whaleal.ddt.execute.config;

import com.alibaba.fastjson2.JSON;
import com.whaleal.icefrog.core.text.CharSequenceUtil;
import com.whaleal.icefrog.core.util.StrUtil;
import com.whaleal.ddt.util.HostInfoUtil;
import lombok.extern.log4j.Log4j2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * 工作信息生成器，用于生成WorkInfo对象
 * 该类负责从配置文件中读取参数，构造WorkInfo对象，并打印配置信息
 */
@Log4j2
public class WorkInfoGenerator {

    /**
     * 定义一个DDL操作集合，用于过滤需要同步的DDL操作
     */

    private static final Set<String> ALL_DDL_SET = new HashSet<>();

    /**
     * 静态代码块，在类加载时初始化ALL_DDL_SET集合
     */
    static {
        ALL_DDL_SET.add("drop");
        ALL_DDL_SET.add("create");
        ALL_DDL_SET.add("createIndexes");
        ALL_DDL_SET.add("dropIndexes");
        ALL_DDL_SET.add("renameCollection");
        ALL_DDL_SET.add("convertToCapped");
        ALL_DDL_SET.add("dropDatabase");
    }

    /**
     * 生成工作信息对象
     *
     * @return 生成的工作信息对象WorkInfo
     */
    public static WorkInfo generateWorkInfo() {
        // 创建一个新的工作信息对象
        WorkInfo workInfo = new WorkInfo();

        // 设置工作名称（从配置文件读取，如果为空则自动生成）
        {
            String workName = Property.getPropertiesByKey("workName");
            if (StrUtil.isBlank(workName)) {
                workName = "work_" + HostInfoUtil.getHostName() + "_" + HostInfoUtil.getProcessID();
                log.warn("workName为空,系统自动生成workName为:" + workName);
            }
            workInfo.setWorkName(workName);
        }

        // 设置源数据源URL（从配置文件读取）
        {
            String sourceDsUrl = Property.getPropertiesByKey("sourceDsUrl");
            if (StrUtil.isBlank(sourceDsUrl)) {
                log.warn("发生严重错误,sourceDsUrl为空,任务将自动退出");
            } else {
                workInfo.setSourceDsUrl(sourceDsUrl);
            }
        }

        // 设置目标数据源URL（从配置文件读取）
        {
            String targetDsUrl = Property.getPropertiesByKey("targetDsUrl");
            if (StrUtil.isBlank(targetDsUrl)) {
                log.warn("发生严重错误,targetDsUrl为空,任务将自动退出");
            } else {
                workInfo.setTargetDsUrl(targetDsUrl);
            }
        }

        // 设置同步模式（从配置文件读取，如果为空或不合法则使用默认模式ALL）
        {
            String syncMode = Property.getPropertiesByKey("syncMode");
            if (CharSequenceUtil.isBlank(syncMode)) {
                log.warn("同步模式为空,采用默认同步模式ALL(全量同步)");
                workInfo.setSyncMode(WorkInfo.SYNC_MODE_ALL);
            } else if (syncMode.equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL) ||
                    syncMode.equalsIgnoreCase(WorkInfo.SYNC_MODE_REAL_TIME) ||
                    syncMode.equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_REAL_TIME) ||
                    syncMode.equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_INCREMENT)) {
                workInfo.setSyncMode(syncMode);
            } else {
                log.warn("同步模式错误,采用默认同步模式ALL(全量同步)");
                workInfo.setSyncMode(WorkInfo.SYNC_MODE_ALL);
            }
        }

        // 设置需要同步的表名（从配置文件读取，如果为空则使用默认值".+"，表示全库全表同步，除了admin、local、config库）
        {
            String dbTableWhite = Property.getPropertiesByKey("dbTableWhite");
            if (CharSequenceUtil.isBlank(dbTableWhite)) {
                workInfo.setDbTableWhite(".+");
                log.warn("同步表名单为空,采用默认全库全表同步(除:admin,local,config库)");
            } else {
                workInfo.setDbTableWhite(dbTableWhite);
            }
        }

        // 设置需要同步的DDL操作（从配置文件读取，如果为空则不进行任何DDL同步）
        {
            String ddlFilterStr = Property.getPropertiesByKey("ddlFilterSet");
            if (CharSequenceUtil.isBlank(ddlFilterStr)) {
                log.warn("同步DDL集合为空,则不进行任何DDL同步");
                workInfo.setDdlFilterSet(new HashSet<>());
            } else {
                String[] split = ddlFilterStr.split(",|，");
                Set<String> ddlSet = new HashSet<>();
                for (String ddlName : split) {
                    if (ALL_DDL_SET.contains(ddlName)) {
                        ddlSet.add(ddlName);
                    } else if ("*".equals(ddlName)) {
                        ddlSet.addAll(ALL_DDL_SET);
                        break;
                    } else {
                        log.warn("无效的DDL操作名称:" + ddlName);
                    }
                }
                if (ddlSet.isEmpty()) {
                    log.warn("同步DDL集合为空,则不进行任何DDL同步");
                    workInfo.setDdlFilterSet(new HashSet<>());
                } else {
                    workInfo.setDdlFilterSet(ddlSet);
                }
            }
        }

        // 设置DDL操作的等待时间（从配置文件读取，如果为空或不合法则使用默认值1200秒）
        {
            String ddlWait = Property.getPropertiesByKey("ddlWait");
            if (CharSequenceUtil.isBlank(ddlWait) || !ddlWait.matches("\\d+")) {
                workInfo.setDdlWait(1200);
                log.warn("ddl等待时间设置错误,使用默认值1200");
            } else {
                workInfo.setDdlWait(Integer.parseInt(ddlWait));
            }
        }

        // 根据同步模式配置线程数
        if (workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_REAL_TIME) ||
                workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_INCREMENT) ||
                workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_REAL_TIME)) {
            // 实时同步情况下总任务线程数（从配置文件读取，如果为空或不合法则使用默认值为当前主机CPU核心数的两倍）
            String realTimeThreadNumStr = Property.getPropertiesByKey("realTimeThreadNum");
            if (CharSequenceUtil.isBlank(realTimeThreadNumStr)) {
                log.warn("实时同步情况下总任务线程数为空,则使用默认值{}", Math.round(HostInfoUtil.computeTotalCpuCore() * 2));
                workInfo.setRealTimeThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 2));
            } else if (!realTimeThreadNumStr.matches("\\d+")) {
                log.warn("实时同步情况下总任务线程数填写错误,则使用默认值{}", Math.round(HostInfoUtil.computeTotalCpuCore() * 2));
                workInfo.setRealTimeThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 2));
            } else {
                int realTimeThreadNum = Integer.parseInt(realTimeThreadNumStr);
                // 设置范围限制，避免线程数过大或过小
                if (realTimeThreadNum > 100 || realTimeThreadNum < 8) {
                    log.warn("实时同步情况下总任务线程数填写错误,则使用默认值{}", Math.round(HostInfoUtil.computeTotalCpuCore() * 2));
                    workInfo.setRealTimeThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 2));
                } else {
                    workInfo.setRealTimeThreadNum(realTimeThreadNum);
                }
            }
        } else if (workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL) ||
                workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_INCREMENT) ||
                workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_REAL_TIME)) {
            // 全量同步情况下的线程数配置
            // 读取源端任务线程数（从配置文件读取，如果为空或不合法则使用默认值为当前主机CPU核心数的0.25倍）
            String sourceThreadNumStr = Property.getPropertiesByKey("sourceThreadNum");
            if (CharSequenceUtil.isBlank(sourceThreadNumStr)) {
                log.warn("全量同步情况下读取源端任务线程数为空,则使用默认值{}", Math.round(HostInfoUtil.computeTotalCpuCore() * 0.25F));
                workInfo.setSourceThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 0.25F));
            } else if (!sourceThreadNumStr.matches("\\d+")) {
                log.warn("全量同步情况下读取源端任务线程数错误,则使用默认值{}", Math.round(HostInfoUtil.computeTotalCpuCore() * 0.25F));
                workInfo.setSourceThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 0.25F));
            } else {
                int sourceThreadNum = Integer.parseInt(sourceThreadNumStr);
                // 设置范围限制，避免线程数过大或过小
                if (sourceThreadNum > 100 || sourceThreadNum < 2) {
                    log.warn("全量同步情况下读取源端任务线程数错误,则使用默认值{}", Math.round(HostInfoUtil.computeTotalCpuCore() * 0.25F));
                    workInfo.setSourceThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 0.25F));
                } else {
                    workInfo.setSourceThreadNum(sourceThreadNum);
                }
            }

            // 写入目标端任务线程数（从配置文件读取，如果为空或不合法则使用默认值为当前主机CPU核心数的0.75倍）
            String targetThreadNumStr = Property.getPropertiesByKey("targetThreadNum");
            if (StrUtil.isBlank(targetThreadNumStr)) {
                log.warn("全量同步情况下写入到目标端任务线程数为空,则使用默认值{}", Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F));
                workInfo.setTargetThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F));
            } else if (!targetThreadNumStr.matches("\\d+")) {
                log.warn("全量同步情况下写入到目标端任务线程数填写错误,则使用默认值{}", Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F));
                workInfo.setTargetThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F));
            } else {
                int targetThreadNum = Integer.parseInt(targetThreadNumStr);
                // 设置范围限制，避免线程数过大或过小
                if (targetThreadNum > 100 || targetThreadNum < 4) {
                    log.warn("全量同步情况下写入到目标端任务线程数填写错误,则使用默认值{}", Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F));
                    workInfo.setTargetThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F));
                } else {
                    workInfo.setTargetThreadNum(targetThreadNum);
                }
            }

            // 全量建立索引的参数信息（从配置文件读取，如果为空或不合法则使用默认值为当前主机CPU核心数）
            String createIndexThreadNumStr = Property.getPropertiesByKey("createIndexThreadNum");
            if (StrUtil.isBlank(createIndexThreadNumStr)) {
                log.warn("全量同步情况下并发建立索引任务线程数为空,则使用默认值{}", Math.round(HostInfoUtil.computeTotalCpuCore()));
                workInfo.setCreateIndexThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore()));
            } else if (!createIndexThreadNumStr.matches("\\d+")) {
                log.warn("全量同步情况下并发建立索引任务线程数填写错误,则使用默认值{}", Math.round(HostInfoUtil.computeTotalCpuCore()));
                workInfo.setCreateIndexThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore()));
            } else {
                int createIndexThreadNum = Integer.parseInt(createIndexThreadNumStr);
                // 设置范围限制，避免线程数过大或过小
                if (createIndexThreadNum > 100 || createIndexThreadNum < 1) {
                    log.warn("全量同步情况下并发建立索引任务线程数填写错误,则使用默认值{}", Math.round(HostInfoUtil.computeTotalCpuCore()));
                    workInfo.setCreateIndexThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore()));
                } else {
                    workInfo.setCreateIndexThreadNum(createIndexThreadNum);
                }
            }
        }

        // 设置每个缓存区缓存批次数量（从配置文件读取，如果为空或不合法则使用默认值20）
        {
            String cacheBucketSizeStr = Property.getPropertiesByKey("bucketSize");
            if (CharSequenceUtil.isBlank(cacheBucketSizeStr)) {
                log.warn("每个缓存区缓存批次数量为空,则使用默认值20");
                workInfo.setBucketSize(20);
            } else if (!cacheBucketSizeStr.matches("\\d+")) {
                log.warn("每个缓存区缓存批次数量填写错误,则使用默认值20");
                workInfo.setBucketSize(20);
            } else {
                int cacheBucketSize = Integer.parseInt(cacheBucketSizeStr);
                workInfo.setBucketSize(cacheBucketSize);
            }
        }

        // 设置缓存区个数（从配置文件读取，如果为空或不合法则使用默认值20）
        {
            String cacheBucketNumStr = Property.getPropertiesByKey("bucketNum");
            if (CharSequenceUtil.isBlank(cacheBucketNumStr)) {
                log.warn("缓存区个数为空,则使用默认值20");
                workInfo.setBucketNum(20);
            } else if (!cacheBucketNumStr.matches("\\d+")) {
                log.warn("缓存区个数填写错误,则使用默认值20");
                workInfo.setBucketNum(20);
            } else {
                int cacheBucketNum = Integer.parseInt(cacheBucketNumStr);
                workInfo.setBucketNum(cacheBucketNum);
            }
        }

        // 设置每批次数据的大小（从配置文件读取，如果为空或不合法则使用默认值128）
        {
            String dataBatchSizeStr = Property.getPropertiesByKey("batchSize");
            if (StrUtil.isBlank(dataBatchSizeStr)) {
                log.warn("每批次数据的大小为空,则使用默认值128");
                workInfo.setBatchSize(128);
            } else if (!dataBatchSizeStr.matches("\\d+")) {
                log.warn("每批次数据的大小填写错误,则使用默认值128");
                workInfo.setBatchSize(128);
            } else {
                int dataBatchSize = Integer.parseInt(dataBatchSizeStr);
                workInfo.setBatchSize(dataBatchSize);
            }
        }

        // 设置oplog的开始时间（从配置文件读取，如果为空或不合法则使用默认值为当前时间的秒数）
        {
            String startOplogTimeStr = Property.getPropertiesByKey("startOplogTime");
            if (StrUtil.isBlank(startOplogTimeStr)) {
                log.warn("oplog的开始时间为空,则使用默认值当前时间");
                workInfo.setStartOplogTime((int) (System.currentTimeMillis() / 1000));
            } else if (!startOplogTimeStr.matches("\\d+") || startOplogTimeStr.length() != 10) {
                log.warn("oplog的开始时间错误,则使用默认值当前时间");
                workInfo.setStartOplogTime((int) (System.currentTimeMillis() / 1000));
            } else {
                int startOplogTime = Integer.parseInt(startOplogTimeStr);
                workInfo.setStartOplogTime(startOplogTime);
            }
        }

        // 设置oplog的结束时间（从配置文件读取，如果为空或不合法则使用默认值0）
        {
            String endOplogTimeStr = Property.getPropertiesByKey("endOplogTime");
            if (StrUtil.isBlank(endOplogTimeStr)) {
                log.warn("oplog的结束时间为空,则使用默认值0");
                workInfo.setEndOplogTime(0);
            } else if (!endOplogTimeStr.matches("\\d+") || endOplogTimeStr.length() != 10) {
                log.warn("oplog的结束时间填写错误,则使用默认值0");
                workInfo.setEndOplogTime(0);
            } else {
                int endOplogTime = Integer.parseInt(endOplogTimeStr);
                workInfo.setEndOplogTime(endOplogTime);
            }
        }

        // 设置延迟时间（从配置文件读取，如果为空或不合法则使用默认值0）
        {
            String delayTimeStr = Property.getPropertiesByKey("delayTime");
            if (StrUtil.isBlank(delayTimeStr)) {
                log.warn("延迟时间为空,则使用默认值0");
                workInfo.setDelayTime(0);
            } else if (!delayTimeStr.matches("\\d+")) {
                log.warn("延迟时间填写错误,则使用默认值0");
                workInfo.setDelayTime(0);
            } else {
                int delayTime = Integer.parseInt(delayTimeStr);
                workInfo.setDelayTime(delayTime);
            }
        }

        // 设置集群信息（从配置文件读取，如果为空则为空集合）
        {
            workInfo.setClusterInfoSet(new HashSet<>(Arrays.asList(Property.getPropertiesByKey("clusterInfoSet").split(",|，"))));
        }

        // 打印工作配置信息
        log.info("work配置信息如下:{}", JSON.toJSONString(workInfo));

        // 返回生成的工作信息对象
        return workInfo;
    }
}
