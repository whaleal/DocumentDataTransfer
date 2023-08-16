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
 * Work Information Generator - Used to generate WorkInfo objects
 * This class is responsible for reading parameters from the configuration file,
 * constructing WorkInfo objects, and printing configuration information.
 */
@Log4j2
public class WorkInfoGenerator {

    // Define a set of DDL operations for filtering the necessary sync DDL operations
    private static final Set<String> ALL_DDL_SET = new HashSet<>();

    // Static block to initialize ALL_DDL_SET set during class loading
    static {
        ALL_DDL_SET.add("drop");
        ALL_DDL_SET.add("create");
        ALL_DDL_SET.add("createIndexes");
        ALL_DDL_SET.add("dropIndexes");
        ALL_DDL_SET.add("renameCollection");
        ALL_DDL_SET.add("convertToCapped");
        ALL_DDL_SET.add("dropDatabase");
    }

    // Generate a work information object
    public static WorkInfo generateWorkInfo() {
        // Create a new work information object
        WorkInfo workInfo = new WorkInfo();

        // Set work name (read from configuration file, generate if empty)
        {
            String workName = Property.getPropertiesByKey("workName");
            if (StrUtil.isBlank(workName)) {
                workName = "work_" + HostInfoUtil.getHostName() + "_" + HostInfoUtil.getProcessID();
                log.warn("workName is empty, system generated workName is: " + workName);
            }
            workInfo.setWorkName(workName);
        }

        // Set source data source URL (read from configuration file)
        {
            String sourceDsUrl = Property.getPropertiesByKey("sourceDsUrl");
            if (StrUtil.isBlank(sourceDsUrl)) {
                log.warn("Serious error occurred, sourceDsUrl is empty, task will exit automatically.");
            } else {
                workInfo.setSourceDsUrl(sourceDsUrl);
            }
        }

        // Set target data source URL (read from configuration file)
        {
            String targetDsUrl = Property.getPropertiesByKey("targetDsUrl");
            if (StrUtil.isBlank(targetDsUrl)) {
                log.warn("Serious error occurred, targetDsUrl is empty, task will exit automatically.");
            } else {
                workInfo.setTargetDsUrl(targetDsUrl);
            }
        }

        // Set sync mode (read from configuration file, use default mode ALL if empty or invalid)
        {
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
        }

        // Set tables to be synchronized (read from configuration file, use default value ".+" for full database-table sync except admin, local, config dbs)
        {
            String dbTableWhite = Property.getPropertiesByKey("dbTableWhite");
            if (CharSequenceUtil.isBlank(dbTableWhite)) {
                workInfo.setDbTableWhite(".+");
                log.warn("Synchronized table list is empty, using default full database-table sync (except: admin, local, config dbs).");
            } else {
                workInfo.setDbTableWhite(dbTableWhite);
            }
        }

        // Set DDL operations to be synchronized (read from configuration file, do not perform any DDL sync if empty)
        {
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
            }
        }

        // Set wait time for DDL operations (read from configuration file, use default value 1200 seconds if empty or invalid)
        {
            String ddlWait = Property.getPropertiesByKey("ddlWait");
            if (CharSequenceUtil.isBlank(ddlWait) || !ddlWait.matches("\\d+")) {
                workInfo.setDdlWait(1200);
                log.warn("DDL wait time setting is incorrect, using default value 1200.");
            } else {
                workInfo.setDdlWait(Integer.parseInt(ddlWait));
            }
        }

        // Configure thread numbers based on sync mode
        if (workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_REAL_TIME) ||
                workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_INCREMENT) ||
                workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_REAL_TIME)) {
            // Configure thread numbers for real-time sync
            {
                String nsBucketThreadNumStr = Property.getPropertiesByKey("nsBucketThreadNum");
                if (CharSequenceUtil.isBlank(nsBucketThreadNumStr)) {
                    log.warn("Thread number for bucket tasks in real-time sync is empty, using default value: {}", workInfo.getNsBucketThreadNum());
                } else if (!nsBucketThreadNumStr.matches("\\d+")) {
                    log.warn("Thread number for bucket tasks in real-time sync is incorrectly filled, using default value: {}", workInfo.getNsBucketThreadNum());
                } else {
                    int nsBucketThreadNum = Integer.parseInt(nsBucketThreadNumStr);
                    // Set limits to avoid excessively large or small thread numbers
                    if (nsBucketThreadNum > 100 || nsBucketThreadNum < 8) {
                        log.warn("Thread number for bucket tasks in real-time sync is incorrect, using default value: {}", workInfo.getNsBucketThreadNum());
                    } else {
                        workInfo.setNsBucketThreadNum(nsBucketThreadNum);
                    }
                }
            }
            {
                String writeThreadNumStr = Property.getPropertiesByKey("writeThreadNum");
                if (CharSequenceUtil.isBlank(writeThreadNumStr)) {
                    log.warn("Thread number for writing data tasks in real-time sync is empty, using default value: {}", workInfo.getWriteThreadNum());
                } else if (!writeThreadNumStr.matches("\\d+")) {
                    log.warn("Thread number for writing data tasks in real-time sync is incorrectly filled, using default value: {}", workInfo.getWriteThreadNum());
                } else {
                    int writeThreadNum = Integer.parseInt(writeThreadNumStr);
                    // Set limits to avoid excessively large or small thread numbers
                    if (writeThreadNum > 100 || writeThreadNum < 8) {
                        log.warn("Thread number for writing data tasks in real-time sync is incorrect, using default value: {}", workInfo.getWriteThreadNum());
                    } else {
                        workInfo.setWriteThreadNum(writeThreadNum);
                    }
                }
            }

            {
                String realTimeTypeStr = Property.getPropertiesByKey("realTimeType");
                if (CharSequenceUtil.isBlank(realTimeTypeStr)) {
                    log.warn("The real-time task synchronization mode is not selected. The default mode is oplog:{}", workInfo.getRealTimeType());
                } else {
                    workInfo.setRealTimeType(realTimeTypeStr);
                }
            }

        } else if (workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL) ||
                workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_INCREMENT) ||
                workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_REAL_TIME)) {
            // Configure thread numbers for full sync
            // Read source-side task thread number (read from configuration file, use default value of 0.25 times the current host's CPU cores)
            String sourceThreadNumStr = Property.getPropertiesByKey("sourceThreadNum");
            if (CharSequenceUtil.isBlank(sourceThreadNumStr)) {
                log.warn("Thread number for source-side tasks in full sync is empty, using default value: {}", Math.round(HostInfoUtil.computeTotalCpuCore() * 0.25F));
                workInfo.setSourceThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 0.25F));
            } else if (!sourceThreadNumStr.matches("\\d+")) {
                log.warn("Thread number for source-side tasks in full sync is incorrect, using default value: {}", Math.round(HostInfoUtil.computeTotalCpuCore() * 0.25F));
                workInfo.setSourceThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 0.25F));
            } else {
                int sourceThreadNum = Integer.parseInt(sourceThreadNumStr);
                // Set limits to avoid excessively large or small thread numbers
                if (sourceThreadNum > 100 || sourceThreadNum < 2) {
                    log.warn("Thread number for source-side tasks in full sync is incorrect, using default value: {}", Math.round(HostInfoUtil.computeTotalCpuCore() * 0.25F));
                    workInfo.setSourceThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 0.25F));
                } else {
                    workInfo.setSourceThreadNum(sourceThreadNum);
                }
            }

            // Configure thread numbers for writing to target (read from configuration file, use default value of 0.75 times the current host's CPU cores)
            String targetThreadNumStr = Property.getPropertiesByKey("targetThreadNum");
            if (StrUtil.isBlank(targetThreadNumStr)) {
                log.warn("Thread number for writing to target tasks in full sync is empty, using default value: {}", Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F));
                workInfo.setTargetThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F));
            } else if (!targetThreadNumStr.matches("\\d+")) {
                log.warn("Thread number for writing to target tasks in full sync is incorrectly filled, using default value: {}", Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F));
                workInfo.setTargetThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F));
            } else {
                int targetThreadNum = Integer.parseInt(targetThreadNumStr);
                // Set limits to avoid excessively large or small thread numbers
                if (targetThreadNum > 100 || targetThreadNum < 4) {
                    log.warn("Thread number for writing to target tasks in full sync is incorrect, using default value: {}", Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F));
                    workInfo.setTargetThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore() * 0.75F));
                } else {
                    workInfo.setTargetThreadNum(targetThreadNum);
                }
            }

            // Configure parameters for building indexes during full sync (read from configuration file, use current host's CPU cores)
            String createIndexThreadNumStr = Property.getPropertiesByKey("createIndexThreadNum");
            if (StrUtil.isBlank(createIndexThreadNumStr)) {
                log.warn("Concurrent index building thread number in full sync is empty, using default value: {}", Math.round(HostInfoUtil.computeTotalCpuCore()));
                workInfo.setCreateIndexThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore()));
            } else if (!createIndexThreadNumStr.matches("\\d+")) {
                log.warn("Concurrent index building thread number in full sync is incorrectly filled, using default value: {}", Math.round(HostInfoUtil.computeTotalCpuCore()));
                workInfo.setCreateIndexThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore()));
            } else {
                int createIndexThreadNum = Integer.parseInt(createIndexThreadNumStr);
                // Set limits to avoid excessively large or small thread numbers
                if (createIndexThreadNum > 100 || createIndexThreadNum < 1) {
                    log.warn("Concurrent index building thread number in full sync is incorrectly filled, using default value: {}", Math.round(HostInfoUtil.computeTotalCpuCore()));
                    workInfo.setCreateIndexThreadNum(Math.round(HostInfoUtil.computeTotalCpuCore()));
                } else {
                    workInfo.setCreateIndexThreadNum(createIndexThreadNum);
                }
            }
        }

        // Set batch count for each cache bucket (read from configuration file, use default value 20)
        {
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
        }

        // Set number of cache buckets (read from configuration file, use default value 20)
        {
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
        }

        // Set data batch size for each batch (read from configuration file, use default value 128)
        {
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
        }

        // Set start time for oplog (read from configuration file, use current time in seconds as default value)
        {
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
        }

        // Set end time for oplog (read from configuration file, use default value 0)
        {
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
        }

        // Set delay time (read from configuration file, use default value 0)
        {
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
        }

        // Set cluster information (read from configuration file, empty set if not provided)
        {
            workInfo.setClusterInfoSet(new HashSet<>(Arrays.asList(Property.getPropertiesByKey("clusterInfoSet").split(",|，"))));
        }

        // Return the generated WorkInfo object
        return workInfo;
    }
}
