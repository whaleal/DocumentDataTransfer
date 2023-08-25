package com.whaleal.ddt.monitor.service.impl;

import com.alibaba.fastjson.JSON;
import com.whaleal.ddt.monitor.model.LogEntity;
import com.whaleal.ddt.monitor.service.LogService;
import com.whaleal.ddt.monitor.service.MonitorDataService;
import com.whaleal.ddt.monitor.service.ParseFileLogService;
import com.whaleal.ddt.monitor.service.WorkService;
import com.whaleal.ddt.monitor.util.DateTimeUtils;
import com.whaleal.ddt.monitor.utilClass.BufferedRandomAccessFile;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

@Service
@Log4j2
public class ParseFileLogServiceImpl implements ParseFileLogService {
    @Autowired
    private WorkService workService;
    @Autowired
    private MonitorDataService monitorDataService;

    /**
     * Maps to store parsed data for full execution.
     */
    private static final Map<Object, Object> fullMap = new TreeMap<>();

    /**
     * Maps to store parsed data for real-time execution.
     */
    private static final Map<Object, Object> realTimeMap = new TreeMap<>();

    /**
     * Maps to store parsed host information.
     */
    private static final Map<Object, Object> hostInfoMap = new TreeMap<>();

    /**
     * Volatile variable to store the host name.
     */
    private static volatile String hostName = "";

    /**
     * Volatile variable to store the process ID.
     */
    private static volatile String pid = "";

    /**
     * Volatile variable to store the boot directory.
     */
    private static volatile String bootDirectory = "";

    /**
     * Volatile variable to store the JVM arguments.
     */
    private static volatile String JVMArg = "";

    /**
     * Volatile variable to store the current work name.
     */
    private static volatile String workName = "";

    /**
     * Volatile variable to store the event time.
     */
    private static volatile double eventTime = 0D;

    /**
     * Volatile variable to store the delay time.
     */
    private static volatile double delayTime = 0D;

    /**
     * Volatile variable to store the incremental progress.
     */
    private static volatile double incProgress = 0D;

    /**
     * Volatile variable to store the current log time.
     */
    private static volatile long logTime = 0;


    /**
     * Reads and parses the log file.
     *
     * @param filePath The path to the log file.
     * @param position The starting position to read from.
     */
    @Override
    public long readFile(String filePath, long position, long endPosition) {
        long filePointerTemp = position;
        File file = new File(filePath);
        try (BufferedRandomAccessFile randomAccessFile = new BufferedRandomAccessFile(file, "r")) {
            randomAccessFile.seek(position);
            String line = "";
            while ((line = randomAccessFile.readLine()) != null) {
                try {
                    if (randomAccessFile.getFilePointer() > endPosition) {
                        break;
                    }
                    // Filter condition: Only process log lines that match the specified regex pattern
                    String regex = "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}) \\[(\\w+)\\] (.*)";
                    if (!line.matches(regex)) {
                        // Skip non-matching lines
                        continue;
                    }
                    parse(logToLogEntity(line));
                } finally {
                    filePointerTemp = randomAccessFile.getFilePointer();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Failed to parse file {}: {}", filePath, e.getMessage());
        }
        return filePointerTemp;
    }

    @Override
    public void readGZFile(String filePath) {
        try (FileInputStream fileInputStream = new FileInputStream(filePath);
             GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
             InputStreamReader streamReader = new InputStreamReader(gzipInputStream, StandardCharsets.UTF_8);
             BufferedReader reader = new BufferedReader(streamReader)) {
            String line;
            while ((line = reader.readLine()) != null) {
                try {
                    // Filter condition: Only process log lines that match the specified regex pattern
                    String regex = "^(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\.\\d{3}) \\[(\\w+)\\] (.*)";
                    if (!line.matches(regex)) {
                        // Skip non-matching lines
                        continue;
                    }
                    parse(logToLogEntity(line));
                } finally {

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }
    }

    public static LogEntity logToLogEntity(String line) {
        LogEntity logEntity = new LogEntity();
        try {
            // Split the log line into individual components
            String[] split = line.split(" +", 8);
            logEntity.setTime(DateTimeUtils.stringToStamp(split[0] + " " + split[1]));
            logEntity.setProcessId(split[2]);
            logEntity.setType(split[5]);
            logEntity.setInfo(split[7]);
            // Set the current log parsing time
            logTime = logEntity.getTime();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Failed to parse log {}: {}", line, e.getMessage());
        }
        return logEntity;
    }


    /**
     * Determines whether to save data based on the log content and process ID.
     *
     * @param logEntity The LogEntity containing the log information.
     */
    public void judgeSave(LogEntity logEntity) {
        // Check if the log is related to a full execution task
        if (logEntity.getProcessId().endsWith("_full_execute]")) {
            // Check if it's the first data record for this task
            if (logEntity.getInfo().contains(" this full task is expected to transfer")) {
                // Save full work data
                monitorDataService.saveFullWorkData(workName, fullMap);
            }
        }

        // Check if the log is related to a real-time execution task
        if (logEntity.getProcessId().endsWith("realTime_execute]")) {
            // Check if it's the first data record for this task
            if (logEntity.getInfo().contains("the total number of event read currently")) {
                // Save real-time work data
                realTimeMap.put("eventTime", eventTime);
                realTimeMap.put("delayTime", delayTime);
                realTimeMap.put("incProgress", incProgress);
                monitorDataService.saveRealTimeWorkData(workName, realTimeMap);
            }
        }

        // Check if the log is related to host information
        if (logEntity.getProcessId().endsWith("hostInfo_print]")) {
            // Check if it's the first data record for host information
            if (logEntity.getInfo().contains("cpu:current system CPU usage:")) {
                // Save host data
                monitorDataService.saveHostData(hostInfoMap);
            }
        }
    }


    /**
     * Extracts the work name from the log message and updates work-related information.
     *
     * @param logEntity The LogEntity containing the log information.
     */
    public void getWorkName(LogEntity logEntity) {
        // Check if the log message indicates the start or end of a task
        String info = logEntity.getInfo();
        if (info.startsWith("enable Start task ") || info.startsWith("end execute task ")) {
            // Parse the JSON data from the log message
            Map<Object, Object> map = JSON.parseObject(info.split(":", 3)[2], Map.class);
            System.out.println(info);
            // Extract work name and update work-related information
            workName = map.get("workName").toString() + "_" + map.get("startTime");
            map.put("workName", workName);
            map.put("hostName", hostName);
            map.put("pid", pid);
            map.put("bootDirectory", bootDirectory);
            map.put("JVMArg", JVMArg);
            // Update work information using the service
            workService.upsertWorkInfo(workName, map);
        }
    }


    /**
     * Parses a LogEntity and populates relevant data into appropriate maps based on log type.
     *
     * @param logEntity The LogEntity containing the log information.
     */
    public void parse(LogEntity logEntity) {
        // Extract and set work name, and save data based on log type
        getWorkName(logEntity);
        judgeSave(logEntity);
        saveLog(logEntity);

        // Process logs based on the process ID
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
        if (logEntity.getProcessId().endsWith("hostInfo_print]") || logEntity.getProcessId().endsWith("hostInfo_limit]")) {
            hostInfoMap.put("workName", workName);
            hostInfoMap.put("createTime", logTime);
            parseHost(logEntity, hostInfoMap);
        }
        if (logEntity.getProcessId().equals("[main]")) {
            parseMain(logEntity);
        }
    }

    @Autowired
    LogService logService;

    public void saveLog(LogEntity logEntity) {
        if (logEntity.getType().equalsIgnoreCase("error") || logEntity.getType().equalsIgnoreCase("error")) {
            logService.saveLog(logEntity);
        }
    }

    /**
     * Parses the main log message containing D2T Boot information and extracts relevant data.
     *
     * @param logEntity The LogEntity containing the log information.
     */
    public static void parseMain(LogEntity logEntity) {
        String info = logEntity.getInfo();

        // Check if the log message contains D2T Boot information
        if (info.startsWith("D2T Boot information :")) {
            // Extract the JSON object from the log message and parse it
            Map<Object, Object> map = JSON.parseObject(info.replaceFirst("D2T Boot information :", ""), Map.class);

            // Extract and store relevant information
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


    /**
     * Parses host-related log messages and populates the provided map with relevant data.
     *
     * @param logEntity The LogEntity containing the log information.
     * @param parse     The map to populate with parsed data.
     */
    public static void parseHost(LogEntity logEntity, Map<Object, Object> parse) {
        String message = logEntity.getInfo();

        // Process memory-related log messages
        if (message.contains("D2T status")) {
            // Check if the log type is "warn" and set isLimit accordingly
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
        }
        // Process CPU-related log messages
        else if (message.startsWith("cpu")) {
            if (message.startsWith("cpu:current system")) {
                parse.put("sysUsage", formStrGetNum(message));
            } else if (message.startsWith("cpu:current idle")) {
                parse.put("sysIdle", formStrGetNum(message));
            } else if (message.startsWith("cpu:current Java")) {
                parse.put("javaCpuUsage", formStrGetNum(message));
            } else if (message.startsWith("cpu:number")) {
                parse.put("threadNum", formStrGetNum(message));
            }
        }
        // Process disk-related log messages
        else if (message.startsWith("netInfo")) {
            // Extract network information from the message
            Map map = JSON.parseObject(message.replaceFirst("netInfo:", ""), Map.class);
            parse.put("recvBytes_" + map.get("name"), map.get("bytesRecv"));
            parse.put("sendBytes_" + map.get("name"), map.get("bytesSent"));
        }
    }

    /**
     * 解析实时任务日志
     *
     * @param logEntity 日志实体类
     * @param parse     存放数据区
     */
    public static void parseRealTask(LogEntity logEntity, Map<Object, Object> parse) {
        // Customized message processing
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
            // Extract and process execution statistics
            Map<String, Object> executionMap = JSON.parseObject(message.split("number of executions:")[1], Map.class);
            parse.put("insert", executionMap.get("insert"));
            parse.put("delete", executionMap.get("delete"));
            parse.put("update", executionMap.get("update"));
            parse.put("cmd", executionMap.get("cmd"));
        } else if (message.contains(" total number of execution")) {
            // Extract and process average write speed
            int startIndex = message.indexOf("average write speed:");
            if (startIndex != -1) {
                String extracted = message.substring(startIndex);
                parse.put("avgWriteSpeed", formStrGetNum(extracted));
            }
        } else if (message.contains(" current round (10s) execution")) {
            // Extract and process real-time write speed
            int startIndex = message.indexOf("execution");
            if (startIndex != -1) {
                String extracted = message.substring(startIndex);
                parse.put("realTimeWriteSpeed", formStrGetNum(extracted));
            }
        } else if (message.contains("current read event time")) {
            // Extract and store current event time
            parse.put("eventTime", formStrGetNum(message));
            eventTime = formStrGetNum(message);
        } else if (message.contains("current event delay time")) {
            // Extract and store current event delay time
            parse.put("delayTime", formStrGetNum(message));
            delayTime = formStrGetNum(message);
        } else if (message.contains("current incremental progress")) {
            // Extract and store current incremental progress
            parse.put("incProgress", formStrGetNum(message));
            incProgress = formStrGetNum(message);
        }

    }

    /**
     * 提取字符串中的第一个数字
     * 没有数字就返回-1
     *
     * @param str 字符串
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


}
