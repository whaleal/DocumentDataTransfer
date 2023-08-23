package com.whaleal.ddt.monitor.service.impl;

import com.alibaba.fastjson.JSON;
import com.whaleal.ddt.monitor.model.LogEntity;
import com.whaleal.ddt.monitor.service.MonitorDataService;
import com.whaleal.ddt.monitor.service.ParseFileLogService;
import com.whaleal.ddt.monitor.service.WorkService;
import com.whaleal.ddt.monitor.util.DateTimeUtils;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
@Log4j2
public class ParseFileLogServiceImpl implements ParseFileLogService {
    @Autowired
    private WorkService workService;
    @Autowired
    private MonitorDataService monitorDataService;

    private static final Map<Object, Object> fullMap = new TreeMap<>();
    private static final Map<Object, Object> realTimeMap = new TreeMap<>();
    private static final Map<Object, Object> hostInfoMap = new TreeMap<>();

    private static volatile String hostName = "";
    private static volatile String pid = "";
    private static volatile String bootDirectory = "";
    private static volatile String JVMArg = "";
    private static volatile String workName = "";

    private static volatile double eventTime = 0D;
    private static volatile double delayTime = 0D;
    private static volatile double incProgress = 0D;

    private static volatile long logTime = 0;


    @Override
    public void readFile(String filePath, long position) {
        File file = new File(filePath);
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                try {
                    // 过滤条件 符合日志输出规范的数据
                    String regex = "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}) \\[(\\w+)\\] (.*)";
                    if (!line.matches(regex)) {
                        continue;
                    }
                    String[] split = line.split(" +", 8);
                    LogEntity logEntity = new LogEntity();
                    logEntity.setTime(DateTimeUtils.stringToStamp(split[0] + " " + split[1]));
                    logEntity.setProcessId(split[2]);
                    logEntity.setType(split[5]);
                    logEntity.setInfo(split[7]);
                    // 设置当前解析日志的时间
                    logTime = logEntity.getTime();
                    parse(logEntity);
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("解析日志{}失败:{}", line, e.getMessage());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("解析文件{},失败:{}", filePath, e.getMessage());
        }
    }

    public void judgeSave(LogEntity logEntity) {
        if (logEntity.getProcessId().endsWith("_full_execute]")) {
            // 判断是否第一条一条数据
            if (logEntity.getInfo().contains(" this full task is expected to transfer")) {
                monitorDataService.saveFullWorkData(workName, fullMap);
            }
        }
        if (logEntity.getProcessId().endsWith("realTime_execute]")) {
            // 判断是否第一条一条数据
            if (logEntity.getInfo().contains("the total number of event read currently")) {
                realTimeMap.put("eventTime", eventTime);
                realTimeMap.put("delayTime", delayTime);
                realTimeMap.put("incProgress", incProgress);
                monitorDataService.saveRealTimeWorkData(workName, realTimeMap);
            }
        }
        if (logEntity.getProcessId().endsWith("hostInfo_print]")) {
            // 判断是否第一条一条数据
            if (logEntity.getInfo().contains("cpu:current system CPU usage:")) {
                monitorDataService.saveHostData(hostInfoMap);
            }
        }
    }

    public void getWorkName(LogEntity logEntity) {
        // 表示一个新的任务出现了
        String info = logEntity.getInfo();
        if (info.startsWith("enable Start task ") || info.startsWith("end execute task ")) {
            Map<Object, Object> map = JSON.parseObject(info.split(":", 3)[2], Map.class);
            System.out.println(info);
            workName = map.get("workName").toString();
            map.put("hostName", hostName);
            map.put("pid", pid);
            map.put("bootDirectory", bootDirectory);
            map.put("JVMArg", JVMArg);
            workService.upsertWorkInfo(workName, map);
        }
        // 判断线程名字
//        if (logEntity.getProcessId().endsWith("_full_execute]") || logEntity.getProcessId().endsWith("_realtime_execute]")) {
//            String workNameTemp = logEntity.getProcessId().split("_")[0];
//            if (!workNameTemp.equals(workName)) {
//                workName = workNameTemp;
//            }
//        }
    }

    public void parse(LogEntity logEntity) {
        getWorkName(logEntity);
        judgeSave(logEntity);

        if (logEntity.getProcessId().endsWith("_full_execute]")) {
            fullMap.put("workName", workName);
            fullMap.put("createTime", logTime);
            parseFullTask(logEntity, fullMap);
        }
        if (logEntity.getProcessId().endsWith("realTime_execute]")) {
            realTimeMap.put("workName", workName);
            realTimeMap.put("createTime", logTime);
            parseRealTask(logEntity, realTimeMap);
        }
        if (logEntity.getProcessId().endsWith("hostInfo_print]")) {
            hostInfoMap.put("workName", workName);
            hostInfoMap.put("createTime", logTime);
            parseHost(logEntity, hostInfoMap);
        }
        if (logEntity.getProcessId().endsWith("hostInfo_limit]")) {
            hostInfoMap.put("workName", workName);
            hostInfoMap.put("createTime", logTime);
            parseHost(logEntity, hostInfoMap);
        }
        if (logEntity.getProcessId().equals("[main]")) {
            parseMain(logEntity);
        }
    }


    public static void parseMain(LogEntity logEntity) {
        String info = logEntity.getInfo();
        if (info.startsWith("D2T Boot information :")) {
            Map<Object, Object> map = JSON.parseObject(info.replaceFirst("D2T Boot information :", ""), Map.class);
            hostName = map.get("hostName").toString();
            pid = map.get("pid").toString();
            bootDirectory = map.get("bootDirectory").toString();
            JVMArg = map.get("JVMArg").toString();
        }

    }

    public static void parseFullTask(LogEntity logEntity, Map<Object, Object> parse) {
        // 定制化处理message
        String message = logEntity.getInfo();
        if (message.contains("this full task is expected to transfer")) {
            parse.put("estimatedTotalNum", formStrGetNum(message));
        } else if (message.contains("current task queue cache status")) {
            parse.put("cacheTaskNum", formStrGetNum(message));
        }
        if (message.contains("the current number of ")) {
            if (message.contains("_readThreadPoolName")) {
                parse.put("readThreadNum", formStrGetNum(message));
            } else if (message.contains("_writeThreadPoolName")) {
                parse.put("writeThreadNum", formStrGetNum(message));
            } else if (message.contains("_writeOfBulkThreadPoolName")) {
                parse.put("writeOfBulkThreadNum", formStrGetNum(message));
            } else if (message.contains("_commonThreadPoolName")) {
                parse.put("commonThreadNum", formStrGetNum(message));
            }
        } else if (message.contains("number of batches remaining in the current buffer")) {
            parse.put("cacheBatchNumber", formStrGetNum(message));
        } else if (message.contains("number of documents remaining in the cache")) {
            parse.put("cacheDocumentNum", formStrGetNum(message));
        } else if (message.contains("number of bars written")) {
            parse.put("writeNum", message.split("[, :]")[5]);
            parse.put("avgWriteSpeed", message.split("[, :]")[12]);
        } else if (message.contains("number of bars read")) {
            parse.put("readNum", message.split("[, :]")[5]);
        } else if (message.contains("the average write speed of this round (10s)")) {
            parse.put("realTimeWriteSpeed", formStrGetNum(message.split(":")[1]));
        }

    }


    public static void parseHost(LogEntity logEntity, Map<Object, Object> parse) {
        String message = logEntity.getInfo();
        // message 中memory 类型的日志处理
        if (message.contains("D2T status")) {
            parse.put("isLimit", "warn".equalsIgnoreCase(logEntity.getType()) ? 1 : 0);
        } else if (message.startsWith("memory")) {
            if (message.startsWith("memory:total memory")) {
                parse.put("totalMemory", formStrGetNum(message));
            } else if (message.startsWith("memory:free heap")) {
                parse.put("freeHeap", formStrGetNum(message));
            } else if (message.startsWith("memory:total heap")) {
                parse.put("totalHeap", formStrGetNum(message));
            } else if (message.startsWith("memory:the available")) {
                parse.put("avaHeap", formStrGetNum(message));
            } else if (message.startsWith("memory:heap memory")) {
                parse.put("useMemory", formStrGetNum(message));
            }
            // message cpu 类型的日志处理
        } else if (message.startsWith("cpu")) {
            if (message.startsWith("cpu:current system")) {
                parse.put("sysUsage", formStrGetNum(message));
            } else if (message.startsWith("cpu:current idle")) {
                parse.put("sysIdle", formStrGetNum(message));
            } else if (message.startsWith("cpu:current Java")) {
                parse.put("javaCpuUsage", formStrGetNum(message));
            } else if (message.startsWith("cpu:number")) {
                parse.put("threadNum", formStrGetNum(message));
            }
            // message disk 类型的日志处理
        } else if (message.startsWith("netInfo")) {
            // key前锥来匹配
            Map map = JSON.parseObject(message.replaceFirst("netInfo:", ""), Map.class);
            parse.put("recvBytes_" + map.get("name"), map.get("bytesRecv"));
            parse.put("sendBytes_" + map.get("name"), map.get("bytesSent"));
        }
    }

    public static void parseRealTask(LogEntity logEntity, Map<Object, Object> parse) {
        // 定制化处理message
        String message = logEntity.getInfo();
        if (message.contains("the current number of ")) {
            if (message.contains("_readEventThreadPoolName")) {
                parse.put("readThreadNum", formStrGetNum(message));
            } else if (message.contains("_parseNSThreadPoolName")) {
                parse.put("parseNSThreadNum", formStrGetNum(message));
            } else if (message.contains("_nsBucketEventThreadPoolName")) {
                parse.put("bucketThreadNum", formStrGetNum(message));
            } else if (message.contains("_writeThreadPoolName")) {
                parse.put("writeThreadNum", formStrGetNum(message));
            }
        } else if (message.contains("the total number of event read currently")) {
            parse.put("readNum", formStrGetNum(message));
        } else if (message.contains(" the current total number of caches")) {
            parse.put("totalCacheNum", formStrGetNum(message));
        } else if (message.contains(" the current number of real-time synchronization caches")) {
            parse.put("cacheNum", formStrGetNum(message));
        } else if (message.contains(" current bucket batch data cache number")) {
            parse.put("bucketBatchNum", formStrGetNum(message));
        } else if (message.contains("current table data cache numbe")) {
            parse.put("nsBatchNum", formStrGetNum(message));
        } else if (message.contains(" current number of synchronization tables")) {
            parse.put("tableNum", formStrGetNum(message));
        } else if (message.contains(" number of executions")) {
            Map map = JSON.parseObject(message.split("number of executions:")[1], Map.class);
            parse.put("insert", map.get("insert"));
            parse.put("delete", map.get("delete"));
            parse.put("update", map.get("update"));
            parse.put("cmd", map.get("cmd"));
        } else if (message.contains(" total number of execution")) {
            int startIndex = message.indexOf("average write speed:");
            if (startIndex != -1) {
                String extracted = message.substring(startIndex);
                parse.put("avgWriteSpeed", formStrGetNum(extracted));
            }
        } else if (message.contains(" current round (10s) execution")) {
            int startIndex = message.indexOf("execution");
            if (startIndex != -1) {
                String extracted = message.substring(startIndex);
                parse.put("realTimeWriteSpeed", formStrGetNum(extracted));
            }
        } else if (message.contains("current read event time")) {
            parse.put("eventTime", formStrGetNum(message));
            eventTime = formStrGetNum(message);
        } else if (message.contains("current event delay time")) {
            parse.put("delayTime", formStrGetNum(message));
            delayTime = formStrGetNum(message);
        } else if (message.contains("current incremental progress")) {
            parse.put("incProgress", formStrGetNum(message));
            incProgress = formStrGetNum(message);
        }
    }

    /**
     * 提取字符串中的数字
     *
     * @param str
     * @return
     */
    public static Double formStrGetNum(String str) {
        double parseDouble = 0;
        // 定义正则表达式和匹配器
        String regex = "(\\d|\\.)+";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(str);
        if (matcher.find()) {
            String sizeInMB = matcher.group();
            parseDouble = Double.parseDouble(sizeInMB);
        } else {
            parseDouble = -1;
        }
        return parseDouble;
    }

    public static void main(String[] args) {
        String str = "{hostName:server190,pid:19582,bootDirectory:/home/lhp/DDT/bin}";
        Map map = JSON.parseObject(str, Map.class);
        System.out.println(map);
    }
}
