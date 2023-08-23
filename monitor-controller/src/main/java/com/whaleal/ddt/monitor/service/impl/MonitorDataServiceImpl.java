package com.whaleal.ddt.monitor.service.impl;


import com.alibaba.fastjson.JSON;
import com.whaleal.ddt.monitor.service.MonitorDataService;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
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
    private  static final  Map<String, Object> MESSAGE_MAP = new HashMap<>();
    static {


        Map<String, Object> hostCPU = new HashMap<>();
        hostCPU.put("javaCpuUsage", "进程cpu使用率");
        hostCPU.put("sysIdle", "系统cpu空闲率");
        hostCPU.put("sysUsage", "系统cpu使用率");
        MESSAGE_MAP.put("hostCPU", hostCPU);


        Map<String, Object> hostMemory = new HashMap<>();
        hostMemory.put("avaHeap", "可用堆栈内存");
        hostMemory.put("freeHeap", "空闲堆栈内存");
        hostMemory.put("totalHeap", "总量堆栈内存");
        hostMemory.put("totalMemory", "总量内存");
        hostMemory.put("useMemory", "已使用内存");
        MESSAGE_MAP.put("hostMemory", hostMemory);


        Map<String, Object> netIO = new HashMap<>();
        netIO.put("recvBytes", "网卡接收流量字节数");
        netIO.put("sendBytes", "网卡发送流量字节数");
        MESSAGE_MAP.put("netIO", netIO);

        Map<String, Object> status = new HashMap<>();
        status.put("isLimit", "是否限速运行:1为限速,0为正常运行");
        MESSAGE_MAP.put("status", status);

        Map<String, Object> fullRate = new HashMap<>();
        fullRate.put("avgWriteSpeed", "平均写入数据");
        fullRate.put("realTimeWriteSpeed", "瞬时写入速度");
        MESSAGE_MAP.put("fullRate", fullRate);


        Map<String, Object> fullThreadNum = new HashMap<>();
        fullThreadNum.put("commonThreadNum", "公共线程数");
        fullThreadNum.put("readThreadNum", "读取task线程数");
        fullThreadNum.put("writeThreadNum", "写入task线程数");
        fullThreadNum.put("writeOfBulkThreadNum", "bulk写入线程数");
        MESSAGE_MAP.put("fullThreadNum", fullThreadNum);


        Map<String, Object> fullCount = new HashMap<>();
        fullCount.put("readNum", "读取数据条数");
        fullCount.put("writeNum", "写入数据条数");
        fullCount.put("estimatedTotalNum", "预估写入总数据条数");
        MESSAGE_MAP.put("fullCount", fullCount);

        Map<String, Object> fullCache = new HashMap<>();
        fullCount.put("cacheBatchNumber", "缓存批次数");
        fullCount.put("cacheDocumentNum", "总缓存数据条数");
        fullCount.put("cacheTaskNum", "缓存读取task数");
        MESSAGE_MAP.put("fullCache", fullCache);

        Map<String, Object> realTimeRate = new HashMap<>();
        realTimeRate.put("avgWriteSpeed", "平均写入数据");
        realTimeRate.put("realTimeWriteSpeed", "瞬时写入速度");
        MESSAGE_MAP.put("realTimeRate", realTimeRate);


        Map<String, Object> realTimeCache = new HashMap<>();
        realTimeCache.put("bucketBatchNum", "分桶区缓存批次数");
        realTimeCache.put("nsBatchNum", "分表区缓存批次数");
        realTimeCache.put("tableNum", "处理ns数");
        realTimeCache.put("totalCacheNum", "总缓存数据条数");
        MESSAGE_MAP.put("realTimeCache", realTimeCache);


        Map<String, Object> realTimeThreadNum = new HashMap<>();
        realTimeThreadNum.put("bucketThreadNum", "分桶线程数");
        realTimeThreadNum.put("parseNSThreadNum", "解析ns线程数");
        realTimeThreadNum.put("readThreadNum", "读取线程数");
        realTimeThreadNum.put("writeThreadNum", "写入线程数");
        MESSAGE_MAP.put("realTimeThreadNum", realTimeThreadNum);


        Map<String, Object> realTimeExecute = new HashMap<>();
        realTimeExecute.put("cmd", "执行DDL数");
        realTimeExecute.put("insert", "插入数");
        realTimeExecute.put("delete", "删除数");
        realTimeExecute.put("update", "更新数");
        MESSAGE_MAP.put("realTimeExecute", realTimeExecute);


        Map<String, Object> realTimeDelayTime = new HashMap<>();
        realTimeDelayTime.put("delayTime", "延迟秒数");
        MESSAGE_MAP.put("realTimeDelayTime", realTimeDelayTime);


        Map<String, Object> realTimeIncProgress = new HashMap<>();
        realTimeIncProgress.put("incProgress", "增量同步进度");
        MESSAGE_MAP.put("realTimeIncProgress", realTimeIncProgress);

    }
    private static String hostInfoDataFile = "hostInfoDataFile.txt";
    private static String fullWorkDataFile = "fullWorkDataFile.txt";
    private static String realTimeWorkDataFile = "realTimeWorkDataFile.txt";

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
        if (map.size() < 10) {
            return;
        }
        try (FileOutputStream fileOutputStream = new FileOutputStream(filePath, true)) {
            fileOutputStream.write((JSON.toJSON(map) + "\r\n").getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
        }
    }


    public Map<String, List<Object>> getMonitor(String filePath, List<String> typeList, long startTime, long endTime) {
        Map<String, List<Object>> resultMap = new HashMap<>();

        for (String type : typeList) {
            resultMap.put(type, new ArrayList<>());
        }
        if (typeList.size() == 3 && typeList.get(2).equals("netIO")) {
            resultMap.remove("netIO");
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Map<String, Object> map = JSON.parseObject(line, Map.class);
                double createTime = Double.parseDouble(map.get("createTime").toString());
                if (createTime <= endTime && createTime >= startTime) {
                    if (typeList.size() == 3 && typeList.get(2).equals("netIO")) {
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
    public Map<String, Object> getWorkMonitor(String workName,
                                              long startTime,
                                              long endTime,
                                              String type) {
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("name", type);
        resultMap.put("message", MESSAGE_MAP.get(type));
        // 必备参数
        List<String> typeList = new ArrayList<>();
        typeList.add("createTime");


        Map<String, List<Object>> monitorData = new HashMap<>();
        if ("hostMemory".equals(type) || "netIO".equals(type) || "hostCPU".equals(type) || "status".equals(type)) {

            if ("hostCPU".equals(type)) {
                typeList.add("javaCpuUsage");
                typeList.add("sysIdle");
                typeList.add("sysUsage");
                resultMap.put("unit", "%");

            }
            if ("hostMemory".equals(type)) {
                typeList.add("avaHeap");
                typeList.add("freeHeap");
                typeList.add("totalHeap");
                typeList.add("totalMemory");
                typeList.add("useMemory");

                resultMap.put("unit", "MB");

            }

            if ("netIO".equals(type)) {
                // 进入下级处理
                typeList.add("netIO");
                resultMap.put("unit", "byte");
            }
            if ("netIO".equals(type)) {
                // 进入下级处理
                typeList.add("netIO");
                resultMap.put("unit", "byte");
            }

            if ("status".equals(type)) {
                typeList.add("isLimit");
                resultMap.put("unit", "boolean");
            }
            monitorData = getHostMonitor(typeList, startTime, endTime);
        }

        if ("fullRate".equals(type) || "fullThreadNum".equals(type) || "fullCount".equals(type)) {
            if ("fullRate".equals(type)) {
                typeList.add("avgWriteSpeed");
                typeList.add("realTimeWriteSpeed");

                resultMap.put("unit", "per");

            }
            if ("fullThreadNum".equals(type)) {
                // 进入下级处理
                typeList.add("commonThreadNum");
                typeList.add("readThreadNum");
                typeList.add("writeThreadNum");
                typeList.add("writeOfBulkThreadNum");

                resultMap.put("unit", "num");

            }
            if ("fullCount".equals(type)) {
                typeList.add("readNum");
                typeList.add("writeNum");
                typeList.add("estimatedTotalNum");

                resultMap.put("unit", "count");

            }
            if ("fullCache".equals(type)) {
                typeList.add("cacheBatchNumber");
                typeList.add("cacheDocumentNum");
                typeList.add("cacheTaskNum");

                resultMap.put("unit", "count");

            }
            monitorData = getFullWorkMonitor(workName, typeList, startTime, endTime);
        }
        if ("realTimeRate".equals(type)
                || "realTimeCache".equals(type)
                || ("realTimeThreadNum".equals(type))
                || "realTimeExecute".equals(type)
                || "realTimeDelayTime".equals(type)
                || "realTimeIncProgress".equals(type)) {
            if ("realTimeRate".equals(type)) {
                typeList.add("avgWriteSpeed");
                typeList.add("realTimeWriteSpeed");
                resultMap.put("unit", "%");
            }
            if ("realTimeCache".equals(type)) {
                typeList.add("bucketBatchNum");
                typeList.add("nsBatchNum");
                typeList.add("tableNum");
                typeList.add("totalCacheNum");
                resultMap.put("unit", "count");
            }


            if ("realTimeThreadNum".equals(type)) {
                typeList.add("bucketThreadNum");
                typeList.add("parseNSThreadNum");
                typeList.add("readThreadNum");
                typeList.add("writeThreadNum");
                resultMap.put("unit", "num");
            }

            if ("realTimeExecute".equals(type)) {
                typeList.add("cmd");
                typeList.add("insert");
                typeList.add("delete");
                typeList.add("update");
                resultMap.put("unit", "num");
            }

            if ("realTimeDelayTime".equals(type)) {
                typeList.add("delayTime");
                resultMap.put("unit", "second");
            }
            if ("realTimeIncProgress".equals(type)) {
                typeList.add("incProgress");
                resultMap.put("unit", "%");
            }
            monitorData = getRealTimeWorkMonitor(workName, typeList, startTime, endTime);

        }


        List<Object> createTimeList = monitorData.containsKey("createTime") ? monitorData.get("createTime") : new ArrayList<>();
        monitorData.remove("createTime");
        resultMap.put("data", monitorData);
        resultMap.put("createTime", createTimeList);
        resultMap.put("isShow", !createTimeList.isEmpty());
        return resultMap;
    }




}
