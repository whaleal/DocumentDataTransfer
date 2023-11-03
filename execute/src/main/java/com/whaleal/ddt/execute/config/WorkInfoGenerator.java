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
package com.whaleal.ddt.execute.config;

import com.whaleal.ddt.util.HostInfoUtil;
import com.whaleal.icefrog.core.text.CharSequenceUtil;
import com.whaleal.icefrog.core.util.StrUtil;
import lombok.extern.log4j.Log4j2;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Work Information Generator - Used to generate WorkInfo objects.
 * This class is responsible for reading parameters from the configuration file,
 * constructing WorkInfo objects, and printing configuration information.
 */
@Log4j2
public class WorkInfoGenerator {

    // Define a set of DDL operations for filtering the necessary sync DDL operations
    private static final Set<String> ALL_DDL_SET = new HashSet<>(Arrays.asList(
            "drop", "create", "createIndexes", "dropIndexes", "renameCollection",
            "convertToCapped", "rename", "shardCollection", "modify"
    ));

    // Generate a work information object
    public static WorkInfo generateWorkInfo() {
        // Create a new work information object
        WorkInfo workInfo = new WorkInfo();

        // Set work name (read from configuration file, generate if empty)
        String workName = Property.getPropertiesByKey("workName");
        if (StrUtil.isBlank(workName)) {
            workName = "workNameDefault";
            log.warn("workName is empty, system-generated workName is: " + workName);
        }
        // Remove digits and underscores from the work name
        workName = workName.replaceAll("\\d+", "").replaceAll("_", "");
        workInfo.setWorkName(workName);

        // Set source data source URL (read from configuration file)
        String sourceDsUrl = Property.getPropertiesByKey("sourceDsUrl");
        if (StrUtil.isBlank(sourceDsUrl)) {
            log.warn("Serious error occurred, sourceDsUrl is empty, task will exit automatically.");
        } else {
            workInfo.setSourceDsUrl(sourceDsUrl);
        }

        // Set target data source URL (read from configuration file)
        String targetDsUrl = Property.getPropertiesByKey("targetDsUrl");
        if (StrUtil.isBlank(targetDsUrl)) {
            log.warn("Serious error occurred, targetDsUrl is empty, task will exit automatically.");
        } else {
            workInfo.setTargetDsUrl(targetDsUrl);
        }

        // Set sync mode (read from configuration file, use default mode ALL if empty or invalid)
        String syncMode = Property.getPropertiesByKey("syncMode");
        if (CharSequenceUtil.isBlank(syncMode)) {
            log.warn("Sync mode is empty, using default sync mode ALL (full sync).");
            workInfo.setSyncMode(WorkInfo.SYNC_MODE_ALL);
        } else if (syncMode.equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL) ||
                syncMode.equalsIgnoreCase(WorkInfo.SYNC_MODE_REAL_TIME) ||
                syncMode.equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_REAL_TIME) ||
                syncMode.equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_INCREMENT)) {
            workInfo.setSyncMode(syncMode);
        } else {
            log.warn("Invalid sync mode, using default sync mode ALL (full sync).");
            workInfo.setSyncMode(WorkInfo.SYNC_MODE_ALL);
        }

        // Set tables to be synchronized (read from configuration file, use default value ".+" for full database-table sync except admin, local, config dbs)
        String dbTableWhite = Property.getPropertiesByKey("dbTableWhite");
        if (CharSequenceUtil.isBlank(dbTableWhite)) {
            workInfo.setDbTableWhite(".+");
            log.warn("Synchronized table list is empty, using default full database-table sync (except: admin, local, config dbs).");
        } else {
            workInfo.setDbTableWhite(dbTableWhite);
        }

        // Set DDL operations to be synchronized (read from configuration file, do not perform any DDL sync if empty)
        String ddlFilterStr = Property.getPropertiesByKey("ddlFilterSet");
        if (CharSequenceUtil.isBlank(ddlFilterStr)) {
            log.warn("DDL sync set is empty, no DDL synchronization will be performed.");
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
                    log.warn("Invalid DDL operation name: " + ddlName);
                }
            }
            if (ddlSet.isEmpty()) {
                log.warn("DDL sync set is empty, no DDL synchronization will be performed.");
                workInfo.setDdlFilterSet(new HashSet<>());
            } else {
                workInfo.setDdlFilterSet(ddlSet);
            }
            if (workInfo.getDdlFilterSet().contains("renameCollection")) {
                workInfo.getDdlFilterSet().add("rename");
            }
            if (workInfo.getDdlFilterSet().contains("createIndexes")) {
                workInfo.getDdlFilterSet().add("commitIndexBuild");
            }
        }

        // Set wait time for DDL operations (read from configuration file, use default value 1200 seconds if empty or invalid)
        String ddlWait = Property.getPropertiesByKey("ddlWait");
        if (CharSequenceUtil.isBlank(ddlWait) || !ddlWait.matches("\\d+")) {
            workInfo.setDdlWait(1200);
            log.warn("DDL wait time setting is incorrect, using default value 1200.");
        } else {
            workInfo.setDdlWait(Integer.parseInt(ddlWait));
        }


        String maxBandwidth = Property.getPropertiesByKey("maxBandwidth");
        if (CharSequenceUtil.isBlank(maxBandwidth) || !maxBandwidth.matches("\\d+")) {
            workInfo.setMaxBandwidth(10);
        } else {
            workInfo.setMaxBandwidth(Integer.parseInt(maxBandwidth));
        }

        String mode = workInfo.getSyncMode();
        // Configure thread numbers based on sync mode
        if (mode.equalsIgnoreCase(WorkInfo.SYNC_MODE_REAL_TIME) ||
                mode.equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_INCREMENT) ||
                mode.equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_REAL_TIME)) {

            // Configure thread numbers for real-time sync
            configureRealTimeThreadNumbers(workInfo);
        }
        if (mode.equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL) ||
                mode.equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_INCREMENT) ||
                mode.equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_REAL_TIME)) {
            // Configure thread numbers for full sync
            configureFullSyncThreadNumbers(workInfo);
        }

        // Set batch count for each cache bucket (read from configuration file, use default value 20)
        String cacheBucketSizeStr = Property.getPropertiesByKey("bucketSize");
        if (CharSequenceUtil.isBlank(cacheBucketSizeStr)) {
            log.warn("Batch count for each cache bucket is empty, using default value: 20");
            workInfo.setBucketSize(20);
        } else if (!cacheBucketSizeStr.matches("\\d+")) {
            log.warn("Batch count for each cache bucket is incorrectly filled, using default value: 20");
            workInfo.setBucketSize(20);
        } else {
            int cacheBucketSize = Integer.parseInt(cacheBucketSizeStr);
            workInfo.setBucketSize(cacheBucketSize);
        }

        // Set number of cache buckets (read from configuration file, use default value 20)
        String cacheBucketNumStr = Property.getPropertiesByKey("bucketNum");
        if (CharSequenceUtil.isBlank(cacheBucketNumStr)) {
            log.warn("Number of cache buckets is empty, using default value: 20");
            workInfo.setBucketNum(20);
        } else if (!cacheBucketNumStr.matches("\\d+")) {
            log.warn("Number of cache buckets is incorrectly filled, using default value: 20");
            workInfo.setBucketNum(20);
        } else {
            int cacheBucketNum = Integer.parseInt(cacheBucketNumStr);
            workInfo.setBucketNum(cacheBucketNum);
        }

        // Set data batch size for each batch (read from configuration file, use default value 128)
        String dataBatchSizeStr = Property.getPropertiesByKey("batchSize");
        if (StrUtil.isBlank(dataBatchSizeStr)) {
            log.warn("Data batch size for each batch is empty, using default value: 128");
            workInfo.setBatchSize(128);
        } else if (!dataBatchSizeStr.matches("\\d+")) {
            log.warn("Data batch size for each batch is incorrectly filled, using default value: 128");
            workInfo.setBatchSize(128);
        } else {
            int dataBatchSize = Integer.parseInt(dataBatchSizeStr);
            workInfo.setBatchSize(dataBatchSize);
        }

        // Set start time for oplog (read from configuration file, use current time in seconds as default value)
        String startOplogTimeStr = Property.getPropertiesByKey("startOplogTime");
        if (StrUtil.isBlank(startOplogTimeStr)) {
            log.warn("Start time for oplog is empty, using default value: current time");
            workInfo.setStartOplogTime((int) (System.currentTimeMillis() / 1000));
        } else if (!startOplogTimeStr.matches("\\d+") || startOplogTimeStr.length() != 10) {
            log.warn("Start time for oplog is incorrect, using default value: current time");
            workInfo.setStartOplogTime((int) (System.currentTimeMillis() / 1000));
        } else {
            int startOplogTime = Integer.parseInt(startOplogTimeStr);
            workInfo.setStartOplogTime(startOplogTime);
        }

        // Set end time for oplog (read from configuration file, use default value 0)
        String endOplogTimeStr = Property.getPropertiesByKey("endOplogTime");
        if (StrUtil.isBlank(endOplogTimeStr)) {
            log.warn("End time for oplog is empty, using default value: 0");
            workInfo.setEndOplogTime(0);
        } else if (!endOplogTimeStr.matches("\\d+") || endOplogTimeStr.length() != 10) {
            log.warn("End time for oplog is incorrectly filled, using default value: 0");
            workInfo.setEndOplogTime(0);
        } else {
            int endOplogTime = Integer.parseInt(endOplogTimeStr);
            workInfo.setEndOplogTime(endOplogTime);
        }

        // Set delay time (read from configuration file, use default value 0)
        String delayTimeStr = Property.getPropertiesByKey("delayTime");
        if (StrUtil.isBlank(delayTimeStr)) {
            log.warn("Delay time is empty, using default value: 0");
            workInfo.setDelayTime(0);
        } else if (!delayTimeStr.matches("\\d+")) {
            log.warn("Delay time is incorrectly filled, using default value: 0");
            workInfo.setDelayTime(0);
        } else {
            int delayTime = Integer.parseInt(delayTimeStr);
            workInfo.setDelayTime(delayTime);
        }

        // Set cluster information (read from configuration file, empty set if not provided)
        workInfo.setClusterInfoSet(new HashSet<>(Arrays.asList(Property.getPropertiesByKey("clusterInfoSet").split(",|，"))));

        // Return the generated WorkInfo object
        return workInfo;
    }

    // Configure thread numbers for real-time sync
    private static void configureRealTimeThreadNumbers(WorkInfo workInfo) {
        // Configure thread numbers for bucket tasks in real-time sync
        int nsBucketThreadNum = configureThreadNumber("nsBucketThreadNum", workInfo.getNsBucketThreadNum(), 8, 100);
        workInfo.setNsBucketThreadNum(nsBucketThreadNum);

        // Configure thread numbers for writing data tasks in real-time sync
        int writeThreadNum = configureThreadNumber("writeThreadNum", workInfo.getWriteThreadNum(), 8, 100);
        workInfo.setWriteThreadNum(writeThreadNum);

        // Set real-time task synchronization mode
        String realTimeTypeStr = Property.getPropertiesByKey("realTimeType");
        if (CharSequenceUtil.isBlank(realTimeTypeStr)) {
            log.warn("The real-time task synchronization mode is not selected. The default mode is oplog: {}", workInfo.getRealTimeType());
        } else {
            workInfo.setRealTimeType(realTimeTypeStr);
        }
    }

    // Configure thread numbers for full sync
    private static void configureFullSyncThreadNumbers(WorkInfo workInfo) {
        // Configure thread numbers for source-side tasks in full sync
        int sourceThreadNum = configureThreadNumber("sourceThreadNum", Math.round(HostInfoUtil.computeTotalCpuCore() * 0.5F), 2, 100);
        workInfo.setSourceThreadNum(sourceThreadNum);

        // Configure thread numbers for writing to target tasks in full sync
        int targetThreadNum = configureThreadNumber("targetThreadNum", Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F), 4, 100);
        workInfo.setTargetThreadNum(targetThreadNum);

        // Configure parameters for building indexes during full sync
        int createIndexThreadNum = configureThreadNumber("createIndexThreadNum", Math.round(HostInfoUtil.computeTotalCpuCore()), 1, 100);
        workInfo.setCreateIndexThreadNum(createIndexThreadNum);

        // Set full task synchronization mode
        String fullTypeStr = Property.getPropertiesByKey("fullType");
        if (CharSequenceUtil.isBlank(fullTypeStr)) {
            log.warn("The full task synchronization mode is not selected. The default mode is oplog: {}", workInfo.getRealTimeType());
        } else {
            workInfo.setFullType(fullTypeStr);
        }
    }

    // Helper method to configure thread numbers with limits
    private static int configureThreadNumber(String propertyKey, int defaultValue, int minLimit, int maxLimit) {
        String threadNumStr = Property.getPropertiesByKey(propertyKey);
        int threadNum;
        if (CharSequenceUtil.isBlank(threadNumStr)) {
            log.warn("Thread number for {} is empty, using default value: {}", propertyKey, defaultValue);
            threadNum = defaultValue;
        } else if (!threadNumStr.matches("\\d+")) {
            log.warn("Thread number for {} is incorrectly filled, using default value: {}", propertyKey, defaultValue);
            threadNum = defaultValue;
        } else {
            threadNum = Integer.parseInt(threadNumStr);
            if (threadNum < minLimit) {
                log.warn("Thread number for {} is too small, adjusting to minimum limit: {}", propertyKey, minLimit);
                threadNum = minLimit;
            } else if (threadNum > maxLimit) {
                log.warn("Thread number for {} is too large, adjusting to maximum limit: {}", propertyKey, maxLimit);
                threadNum = maxLimit;
            }
        }

        return threadNum;
    }
}
