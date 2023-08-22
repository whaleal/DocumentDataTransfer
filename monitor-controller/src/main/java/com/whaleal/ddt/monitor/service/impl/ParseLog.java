package com.whaleal.ddt.monitor.service.impl;

import com.alibaba.fastjson.JSON;
import com.whaleal.ddt.monitor.model.LogEntity;
import com.whaleal.ddt.monitor.util.DateTimeUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @projectName: full-common
 * @package: com.whaleal.ddt.monitor.task
 * @className: ReadFIle
 * @author: Eric
 * @description: TODO
 * @date: 21/08/2023 16:18
 * @version: 1.0
 */
public class ParseLog {

    private static final Map<String, Map<Object, Object>> WORK_INFO_MAP = new ConcurrentHashMap<>();

    private static final Map<String, Object> fullMap = new TreeMap<>();
    private static final Map<String, Object> realTimeMap = new TreeMap<>();
    private static final Map<String, Object> hostInfoMap = new TreeMap<>();

    private static String hostName = "";
    private static String pid = "";
    private static String bootDirectory = "";
    private static String JVMInfo = "";
    private static String workName = "";

    public static void main(String[] args) {
        readFile("/Users/liheping/Desktop/log.log");
    }

    public static void readFile(String filePath) {
        File file = new File(filePath);
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] split = line.split(" +", 8);
                LogEntity logEntity = new LogEntity();
                if (split.length < 8) {
                    continue;
                }
                String regex = "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}) \\[(\\w+)\\] (.*)";

                if (!line.matches(regex)) {
                    continue;
                }

                logEntity.setTime(DateTimeUtils.stringToStamp(split[0] + " " + split[1]));
                logEntity.setProcessId(split[2]);
                logEntity.setType(split[5]);
                logEntity.setInfo(split[7]);
                // 表示一个新的任务出现了
                String info = logEntity.getInfo();
                if (info.startsWith("enable Start task ")) {
                    Map map = JSON.parseObject(info.split(":", 3)[2], Map.class);
                    WORK_INFO_MAP.put(map.get("workName").toString(), map);
                    workName = map.get("workName").toString();
                }

                if (logEntity.getProcessId().endsWith("_full_execute]")) {
                    // 判断是否第一条一条数据
                    if (logEntity.getInfo().contains(" this full task is expected to transfer")) {
                        // todo
                        // 保存数据
                    }
                    fullMap.put("workName", workName);
                    parseFullTask(logEntity, fullMap);
                }
                if (logEntity.getProcessId().endsWith("_realtime_execute]")) {
                    if (logEntity.getInfo().contains("the total number of event read currently")) {
                        // todo
                        // 保存数据
                    }
                    fullMap.put("workName", workName);
                    parseRealTask(logEntity, realTimeMap);
                }
                if (logEntity.getProcessId().endsWith("hostInfo_print]")) {
                    if (logEntity.getInfo().contains("tcpu:current system CPU usage:")) {
                        // todo
                        // 保存数据
                    }
                    fullMap.put("workName", workName);
                    parseHost(logEntity, hostInfoMap);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void parse() {

    }

    public static void parseFullTask(LogEntity logEntity, Map<String, Object> parse) {
        //定制化处理message
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


    public static void parseHost(LogEntity logEntity, Map<String, Object> parse) {
        String message = logEntity.getInfo();
        // message 中memory 类型的日志处理
        if (message.startsWith("memory")) {
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
            parse.put("recvBytesOf" + map.get("name"), map.get("bytesRecv"));
            parse.put("sendBytesOf" + map.get("name"), map.get("bytesSent"));
        }
    }

    public static void parseRealTask(LogEntity logEntity, Map<String, Object> parse) {
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
            Map map = JSON.parseObject(message.replaceFirst("number of executions:", ""), Map.class);
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
            // todo 不一定有数据
            parse.put("eventTime", formStrGetNum(message));
        } else if (message.contains("current event delay time")) {
            parse.put("delayTime", formStrGetNum(message));
        } else if (message.contains("current incremental progress")) {
            parse.put("incProgress", formStrGetNum(message));
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
            ;
        } else {
            parseDouble = -1;
        }
        return parseDouble;
    }
}
