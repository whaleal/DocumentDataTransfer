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
package com.whaleal.ddt.monitor.service.impl;


import com.alibaba.fastjson.JSON;
import com.whaleal.ddt.monitor.service.MonitorDataService;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author liheping
 */
@Service
public class MonitorDataServiceImpl implements MonitorDataService {

    private static final Map<String, Object> MESSAGE_MAP = new HashMap<>();

    static {
        // Create a map to store CPU usage metrics and their descriptions
        Map<String, Object> hostCPU = new HashMap<>();
        hostCPU.put("javaCpuUsage", "Process CPU Usage");
        hostCPU.put("sysIdle", "System CPU Idle Rate");
        hostCPU.put("sysUsage", "System CPU Usage Rate");
        MESSAGE_MAP.put("hostCPU", hostCPU);

// Create a map to store memory usage metrics and their descriptions
        Map<String, Object> hostMemory = new HashMap<>();
        hostMemory.put("avaHeap", "Available Heap Memory");
        hostMemory.put("freeHeap", "Free Heap Memory");
        hostMemory.put("totalHeap", "Total Heap Memory");
        hostMemory.put("totalMemory", "Total Memory");
        hostMemory.put("useMemory", "Used Memory");
        MESSAGE_MAP.put("hostMemory", hostMemory);

// Create a map to store network I/O metrics and their descriptions
        Map<String, Object> netIO = new HashMap<>();
        netIO.put("recvBytes", "Network Received Bytes");
        netIO.put("sendBytes", "Network Sent Bytes");
        MESSAGE_MAP.put("netIO", netIO);

// Create a map to store status metrics and their descriptions
        Map<String, Object> status = new HashMap<>();
        status.put("isLimit", "Is Limited Speed Running: 1 for Limited, 0 for Normal");
        MESSAGE_MAP.put("status", status);

// Create a map to store full synchronization rate metrics and their descriptions
        Map<String, Object> fullRate = new HashMap<>();
        fullRate.put("avgWriteSpeed", "Average Write Speed");
        fullRate.put("realTimeWriteSpeed", "RealTime write Speed");
        MESSAGE_MAP.put("fullRate", fullRate);

// Create a map to store thread count metrics for full synchronization and their descriptions
        Map<String, Object> fullThreadNum = new HashMap<>();
        fullThreadNum.put("commonThreadNum", "Common Thread Count");
        fullThreadNum.put("readThreadNum", "Read Task Thread Count");
        fullThreadNum.put("writeThreadNum", "Write Task Thread Count");
        fullThreadNum.put("writeOfBulkThreadNum", "Bulk Write Thread Count");
        MESSAGE_MAP.put("fullThreadNum", fullThreadNum);

// Create a map to store data count metrics for full synchronization and their descriptions
        Map<String, Object> fullCount = new HashMap<>();
        fullCount.put("readNum", "Read Data Count");
        fullCount.put("writeNum", "Write Data Count");
        fullCount.put("estimatedTotalNum", "Estimated Total Write Data Count");
        MESSAGE_MAP.put("fullCount", fullCount);

// Create a map to store caching metrics for full synchronization and their descriptions
        Map<String, Object> fullCache = new HashMap<>();
        fullCache.put("cacheBatchNumber", "Cache Batch Count");
        fullCache.put("cacheDocumentNum", "Total Cached Data Count");
        fullCache.put("cacheTaskNum", "Cache Read Task Count");
        MESSAGE_MAP.put("fullCache", fullCache);

// Create a map to store real-time synchronization rate metrics and their descriptions
        Map<String, Object> realTimeRate = new HashMap<>();
        realTimeRate.put("avgWriteSpeed", "Average Write Speed");
        realTimeRate.put("realTimeWriteSpeed", "RealTime Write Speed");
        MESSAGE_MAP.put("realTimeRate", realTimeRate);

// Create a map to store caching metrics for real-time synchronization and their descriptions
        Map<String, Object> realTimeCache = new HashMap<>();
        realTimeCache.put("bucketBatchNum", "Bucket Area Cache Batch Count");
        realTimeCache.put("nsBatchNum", "Table Area Cache Batch Count");
        realTimeCache.put("tableNum", "Processed NS Count");
        realTimeCache.put("totalCacheNum", "Total Cached Data Count");
        MESSAGE_MAP.put("realTimeCache", realTimeCache);

// Create a map to store thread count metrics for real-time synchronization and their descriptions
        Map<String, Object> realTimeThreadNum = new HashMap<>();
        realTimeThreadNum.put("bucketThreadNum", "Bucket Thread Count");
        realTimeThreadNum.put("parseNSThreadNum", "Parse NS Thread Count");
        realTimeThreadNum.put("readThreadNum", "Read Thread Count");
        realTimeThreadNum.put("writeThreadNum", "Write Thread Count");
        MESSAGE_MAP.put("realTimeThreadNum", realTimeThreadNum);

// Create a map to store execution metrics for real-time synchronization and their descriptions
        Map<String, Object> realTimeExecute = new HashMap<>();
        realTimeExecute.put("cmd", "DDL Execution Count");
        realTimeExecute.put("insert", "Insertion Count");
        realTimeExecute.put("delete", "Deletion Count");
        realTimeExecute.put("update", "Update Count");
        MESSAGE_MAP.put("realTimeExecute", realTimeExecute);

// Create a map to store delay time metrics for real-time synchronization and their descriptions
        Map<String, Object> realTimeDelayTime = new HashMap<>();
        realTimeDelayTime.put("delayTime", "Delay Time in Seconds");
        MESSAGE_MAP.put("realTimeDelayTime", realTimeDelayTime);

// Create a map to store incremental progress metrics for real-time synchronization and their descriptions
        Map<String, Object> realTimeIncProgress = new HashMap<>();
        realTimeIncProgress.put("incProgress", "Incremental Sync Progress");
        MESSAGE_MAP.put("realTimeIncProgress", realTimeIncProgress);

    }

    private static String monitorDataDir = "../monitorDataDir/";
    private static String hostInfoDataFile = "hostInfoDataFile.txt";
    private static String fullWorkDataFile = "fullWorkDataFile.txt";
    private static String realTimeWorkDataFile = "realTimeWorkDataFile.txt";

    static {
        File file = new File(monitorDataDir);
        file.delete();
        file.deleteOnExit();
        file.mkdir();
        file.mkdirs();
    }

    @Override
    public void saveHostData(Map<Object, Object> map) {
        saveData(hostInfoDataFile, map);
    }

    @Override
    public void saveFullWorkData(String workName, Map<Object, Object> map) {
        map.put("workName", workName);
        saveData(workName + "_" + fullWorkDataFile, map);
    }

    @Override
    public void saveRealTimeWorkData(String workName, Map<Object, Object> map) {
        map.put("workName", workName);
        saveData(workName + "_" + realTimeWorkDataFile, map);
    }

    private static void saveData(String filePath, Map<Object, Object> map) {
        filePath = monitorDataDir + filePath;
        if (map.size() < 10) {
            return;
        }
        try (FileOutputStream fileOutputStream = new FileOutputStream(filePath, true)) {
            fileOutputStream.write((JSON.toJSON(map) + "\r\n").getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
        }
    }


    public static Map<String, List<Object>> getMonitor(String filePath, List<String> typeList, long startTime, long endTime) {
        filePath = monitorDataDir + filePath;
        Map<String, List<Object>> resultMap = new HashMap<>();
        for (String type : typeList) {
            resultMap.put(type, new ArrayList<>());
        }
        if (typeList.contains("netIO")) {
            resultMap.remove("netIO");
        }
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Map<String, Object> map = JSON.parseObject(line, Map.class);

                double createTime = Double.parseDouble(map.get("createTime").toString());
                if (createTime <= endTime && createTime >= startTime) {
                    if (typeList.contains("netIO")) {
                        for (Map.Entry<String, Object> entry : map.entrySet()) {
                            String key = entry.getKey();
                            if (key.startsWith("sendBytes")) {
                                if (!resultMap.containsKey(key)) {
                                    resultMap.put(key, new ArrayList<>());
                                }
                                resultMap.get(key).add(entry.getValue());
                            }
                            if (key.startsWith("recvBytes")) {
                                if (!resultMap.containsKey(key)) {
                                    resultMap.put(key, new ArrayList<>());
                                }
                                resultMap.get(key).add(entry.getValue());
                            }
                            if (key.equals("createTime")) {
                                resultMap.get("createTime").add(Long.parseLong(entry.getValue().toString()));
                            }
                        }

                    } else {
                        for (String type : typeList) {
                            String value = map.get(type).toString();
                            if (type.equals("createTime")) {
                                resultMap.get(type).add(Long.parseLong(value));
                            } else {
                                resultMap.get(type).add(Double.parseDouble(value));
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
        }

        return resultMap;
    }

    @Override
    public Map<String, List<Object>> getHostMonitor(List<String> typeList, long startTime, long endTime) {
        return getMonitor(hostInfoDataFile, typeList, startTime, endTime);
    }

    @Override
    public Map<String, List<Object>> getFullWorkMonitor(String workName, List<String> typeList, long startTime, long endTime) {
        return getMonitor(workName + "_" + fullWorkDataFile, typeList, startTime, endTime);
    }

    @Override
    public Map<String, List<Object>> getRealTimeWorkMonitor(String workName, List<String> typeList, long startTime, long endTime) {
        return getMonitor(workName + "_" + realTimeWorkDataFile, typeList, startTime, endTime);
    }


    @Override
    public Map<String, Object> getWorkMonitor(String workName, long startTime, long endTime, String type) {
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("name", type);
        resultMap.put("message", MESSAGE_MAP.get(type));

        List<String> typeList = new ArrayList<>();
        typeList.add("createTime");

        String unit = "";
        Map<String, List<Object>> monitorData = new HashMap<>();

        switch (type) {
            case "hostCPU":
            case "hostMemory":
            case "netIO":
            case "status":
                unit = handleHostType(type, typeList, resultMap);
                monitorData = getHostMonitor(typeList, startTime, endTime);
                break;
            case "fullRate":
            case "fullThreadNum":
            case "fullCount":
            case "fullCache":
                unit = handleFullType(type, typeList, resultMap);
                monitorData = getFullWorkMonitor(workName, typeList, startTime, endTime);
                break;
            case "realTimeRate":
            case "realTimeCache":
            case "realTimeThreadNum":
            case "realTimeExecute":
            case "realTimeDelayTime":
            case "realTimeIncProgress":
                unit = handleRealTimeType(type, typeList, resultMap);
                monitorData = getRealTimeWorkMonitor(workName, typeList, startTime, endTime);
                break;
            default:
                // Handle the case where 'type' is not recognized
                break;
        }

        List<Object> createTimeList = monitorData.containsKey("createTime") ? monitorData.get("createTime") : new ArrayList<>();
        monitorData.remove("createTime");
        resultMap.put("data", monitorData);
        resultMap.put("createTime", createTimeList);
        resultMap.put("isShow", !createTimeList.isEmpty());
        resultMap.put("unit", unit);

        return resultMap;
    }

// Define separate methods for each type category

    private String handleHostType(String type, List<String> typeList, Map<String, Object> resultMap) {
        String unit = "";
        if ("hostCPU".equals(type)) {
            typeList.add("javaCpuUsage");
            typeList.add("sysIdle");
            typeList.add("sysUsage");
            unit = "%";
        } else if ("hostMemory".equals(type)) {
            typeList.add("avaHeap");
            typeList.add("freeHeap");
            typeList.add("totalHeap");
            typeList.add("totalMemory");
            typeList.add("useMemory");
            unit = "MB";
        } else if ("netIO".equals(type)) {
            typeList.add("netIO");
            unit = "byte";
        } else if ("status".equals(type)) {
            typeList.add("isLimit");
            unit = "boolean";
        }
        resultMap.put("unit", unit);
        return unit;
    }

    private String handleFullType(String type, List<String> typeList, Map<String, Object> resultMap) {
        String unit = "";
        if ("fullRate".equals(type)) {
            typeList.add("avgWriteSpeed");
            typeList.add("realTimeWriteSpeed");
            unit = "per";
        } else if ("fullThreadNum".equals(type)) {
            typeList.add("commonThreadNum");
            typeList.add("readThreadNum");
            typeList.add("writeThreadNum");
            typeList.add("writeOfBulkThreadNum");
            unit = "num";
        } else if ("fullCount".equals(type)) {
            typeList.add("readNum");
            typeList.add("writeNum");
            typeList.add("estimatedTotalNum");
            unit = "count";
        } else if ("fullCache".equals(type)) {
            typeList.add("cacheBatchNumber");
            typeList.add("cacheDocumentNum");
            typeList.add("cacheTaskNum");
            unit = "count";
        }
        resultMap.put("unit", unit);
        return unit;
    }

    private String handleRealTimeType(String type, List<String> typeList, Map<String, Object> resultMap) {
        String unit = "";
        if ("realTimeRate".equals(type)) {
            typeList.add("avgWriteSpeed");
            typeList.add("realTimeWriteSpeed");
            unit = "%";
        } else if ("realTimeCache".equals(type)) {
            typeList.add("bucketBatchNum");
            typeList.add("nsBatchNum");
            typeList.add("tableNum");
            typeList.add("totalCacheNum");
            unit = "count";
        } else if ("realTimeThreadNum".equals(type)) {
            typeList.add("bucketThreadNum");
            typeList.add("parseNSThreadNum");
            typeList.add("readThreadNum");
            typeList.add("writeThreadNum");
            unit = "num";
        } else if ("realTimeExecute".equals(type)) {
            typeList.add("cmd");
            typeList.add("insert");
            typeList.add("delete");
            typeList.add("update");
            unit = "num";
        } else if ("realTimeDelayTime".equals(type)) {
            typeList.add("delayTime");
            unit = "second";
        } else if ("realTimeIncProgress".equals(type)) {
            typeList.add("incProgress");
            unit = "%";
        }
        resultMap.put("unit", unit);
        return unit;
    }

}
