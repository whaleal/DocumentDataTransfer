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
package com.whaleal.ddt.util;


import com.alibaba.fastjson2.JSON;
import com.sun.management.OperatingSystemMXBean;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;
import oshi.SystemInfo;
import oshi.hardware.HWDiskStore;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * @author liheping
 */
@Log4j2
@Getter
public class HostInfoUtil {
    /**
     * 全局限速标识
     */
    private static volatile Boolean isLimit = Boolean.FALSE;

    private static final String MEGABYTES = " MB";

    private static final int WARNING_THRESHOLD = 80;

    /**
     * 启动一个后台线程周期性地打印主机信息。
     */
    static {
        final Thread thread = new Thread(() -> {
            while (true) {
                try {
                    // 30s输出一次主机信息
                    TimeUnit.SECONDS.sleep(30);
                   // printHostInfo();
                } catch (Exception ignored) {

                }
            }
        });
        thread.setName("hostInfo_print");
        thread.start();
    }

    static {
        final Thread thread = new Thread(() -> {
            while (true) {
                try {
                    TimeUnit.SECONDS.sleep(10);
                    boolean isLimitTemp = false;
                    // 10s判断一下 是否要进行限速代码
                    if (computeJvmMemoryOverLoad() || computeHostMemoryOverLoad()) {
                        isLimit = true;
                        Runtime.getRuntime().gc();
                        log.info("D2T description The memory is overloaded, and the rate limit is enabled");
                        isLimitTemp = true;
                    }
                    if (computeJvmCpuOverLoad() || computeHostCpuOverLoad()) {
                        isLimit = true;
                        Runtime.getRuntime().gc();
                        log.info("D2T the cpu is overloaded, and the rate limit is enabled. Procedure");
                        isLimitTemp = true;
                    }
                    if (!isLimitTemp) {
                        isLimit = false;
                    }
                } catch (Exception ignored) {
                }
            }
        });
        thread.setName("hostInfo_limit");
        thread.start();
    }

    /**
     * 打印当前JVM的内存信息。
     */
    private static void printMemory() {
        // 获取当前JVM的内存信息
        long totalMemory = Runtime.getRuntime().totalMemory();
        long freeMemory = Runtime.getRuntime().freeMemory();
        long maxMemory = Runtime.getRuntime().maxMemory();
        long usedMemory = totalMemory - freeMemory;
        // 记录已分配总堆内存
        log.info("memory:total heap memory allocated:{} MB", totalMemory / 1024 / 1024);
        // 记录已分配空闲堆内存
        log.info("memory:free heap memory has been allocated:{} MB", freeMemory / 1024 / 1024);
        // 记录总内存大小
        log.info("memory:total memory size:{} MB", maxMemory / 1024 / 1024);
        // 计算可用堆内存大小并记录
        long useAbleMemory = (maxMemory - totalMemory + freeMemory) / 1024 / 1024;
        log.info("memory:the available heap memory size:{} MB", useAbleMemory);
        // 如果使用内存超过预设的阈值，记录警告日志
        if (Math.round(usedMemory / (totalMemory + 0.0F)) > WARNING_THRESHOLD) {
            log.warn("memory:heap memory is used:{} MB ({}%)", usedMemory / 1024 / 1024, Math.round(usedMemory / (totalMemory + 0.0F)));
        } else {
            // 记录已使用堆内存和使用百分比
            log.info("memory:heap memory is used:{} MB ({}%)", usedMemory / 1024 / 1024, Math.round(usedMemory / (totalMemory + 0.0F)));
        }
    }

    /**
     * 打印当前系统和Java程序的CPU使用率，以及JVM中的线程数量。
     */
    private static void printCPU() {
        {
            OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
            // 获取当前系统CPU使用率
            double systemCpuUsage = osBean.getSystemCpuLoad() * 100;
            if (systemCpuUsage > WARNING_THRESHOLD) {
                log.warn("cpu:current system CPU usage:" + systemCpuUsage + "%");
            } else {
                log.info("cpu:current system CPU usage:" + systemCpuUsage + "%");
            }
            // 获取当前系统CPU空闲率
            double systemCpuIdle = 100 - systemCpuUsage;
            log.info("cpu:current idle CPU rate of the system:" + systemCpuIdle + "%");

            // 获取当前Java程序CPU使用率
            double processCpuUsage = osBean.getProcessCpuLoad() * 100;
            if (processCpuUsage > WARNING_THRESHOLD) {
                log.warn("cpu:current Java program CPU usage:" + processCpuUsage + "%");
            } else {
                log.info("cpu:current Java program CPU usage:" + processCpuUsage + "%");
            }
        }
        {
            ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
            int threadCount = threadMXBean.getThreadCount();
            log.info("cpu:number of threads in the JVM:" + threadCount);
        }
    }

    /**
     * 打印JVM中的线程信息，包括线程ID、状态和堆栈信息。
     */
    private static void printThreadInfo() {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadMXBean.getAllThreadIds();
        ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds, true, true);
        for (ThreadInfo threadInfo : threadInfos) {
            Map<String, Object> threadInfoMap = new HashMap<>();
            threadInfoMap.put("name", threadInfo.getThreadName());
            threadInfoMap.put("id", threadInfo.getThreadId());
            threadInfoMap.put("state", threadInfo.getThreadState());
            List<String> stackList = new ArrayList<>();
            StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
            for (StackTraceElement stackTraceElement : stackTraceElements) {
                stackList.add(stackTraceElement.toString());
            }
            threadInfoMap.put("stack", stackList);
            log.info("threadInfo:{}", JSON.toJSONString(threadInfoMap));
        }
    }

    /**
     * 打印磁盘IO信息。
     */
    private static void printDiskIOInfo() {
        try {
            SystemInfo systemInfo = new SystemInfo();
            HardwareAbstractionLayer hardware = systemInfo.getHardware();
            HWDiskStore[] diskStores = hardware.getDiskStores();
            for (HWDiskStore disk : diskStores) {
                Map<String, Object> diskIOInfoMap = new HashMap<>();
                diskIOInfoMap.put("name", disk.getName());
                diskIOInfoMap.put("readBytes", disk.getReadBytes());
                diskIOInfoMap.put("writeBytes", disk.getWriteBytes());
                log.info("diskInfo:" + JSON.toJSONString(diskIOInfoMap));
            }
        } catch (Exception ignored) {

        }
    }

    /**
     * 打印网络IO信息。
     */

    private static void printNetIOInfo() {
        try {
            Map<String, Map<String, Object>> networkMap = new HashMap<>();
            {
                SystemInfo systemInfo = new SystemInfo();
                NetworkIF[] networkIFs = systemInfo.getHardware().getNetworkIFs();
                for (NetworkIF networkIF : networkIFs) {
                    Map<String, Object> NetworkInfoMap = new HashMap<>();
                    NetworkInfoMap.put("name", networkIF.getName());
                    NetworkInfoMap.put("bytesRecv", networkIF.getBytesRecv());
                    NetworkInfoMap.put("bytesSent", networkIF.getBytesSent());
                    networkMap.put(networkIF.getName(), NetworkInfoMap);
                }
            }

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            {
                SystemInfo systemInfo = new SystemInfo();
                NetworkIF[] networkIFs = systemInfo.getHardware().getNetworkIFs();
                for (NetworkIF networkIF : networkIFs) {
                    long bytesRecv = networkIF.getBytesRecv();
                    long bytesSent = networkIF.getBytesSent();
                    long oldR = Long.parseLong(networkMap.get(networkIF.getName()).get("bytesRecv").toString());
                    long oldS = Long.parseLong(networkMap.get(networkIF.getName()).get("bytesSent").toString());
                    networkMap.get(networkIF.getName()).put("bytesRecv", bytesRecv - oldR);
                    networkMap.get(networkIF.getName()).put("bytesSent", bytesSent - oldS);
                }
                for (Map.Entry<String, Map<String, Object>> entry : networkMap.entrySet()) {
                    log.info("netInfo:{}", JSON.toJSONString(entry.getValue()));
                }
            }
        } catch (Exception ignored) {

        }
    }

    /**
     * 计算总CPU核心数。
     *
     * @return 总CPU核心数。
     */
    public static int computeTotalCpuCore() {
        int processors = Runtime.getRuntime().availableProcessors();
        if (processors > 40) {
            return 40;
        }
        if (processors <= 8) {
            return 16;
        }
        return processors;
    }

    /**
     * 打印主机信息（CPU、内存、线程、磁盘IO和网络IO）。
     */
    public static void printHostInfo() {
        try {
            printCPU();
            printMemory();
            // printThreadInfo();
           // printDiskIOInfo();
            printNetIOInfo();
        } catch (Exception ignored) {

        }
    }

    /**
     * 获取当前JVM的进程ID。
     *
     * @return 当前JVM的进程ID。
     */

    public static String getProcessID() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        return runtimeMXBean.getName().split("@", 2)[0];
    }

    /**
     * 获取当前JVM的主机名。
     *
     * @return 当前JVM的主机名。
     */
    public static String getHostName() {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        return runtimeMXBean.getName().split("@", 2)[1];
    }

    /**
     * 获取当前进程的工作目录。
     *
     * @return 当前进程的工作目录。
     */
    public static String getProcessDir() {
        return System.getProperty("user.dir");
    }

    /**
     * 获取JVM启动参数列表
     *
     * @return List<String>
     */
    public static List<String> getJvmArg() {
        // 获取RuntimeMXBean实例
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        // 获取JVM启动参数列表
        List<String> jvmArglist = runtimeMXBean.getInputArguments();
        return jvmArglist;
    }

    /**
     * 计算主机的 CPU 负载情况。
     * 使用 OperatingSystemMXBean 获取系统 CPU 负载，并计算 CPU 使用率百分比。
     *
     * @return 如果主机 CPU 使用率大于 WARNING_THRESHOLD%，则返回 true，否则返回 false。
     */
    public static boolean computeHostCpuOverLoad() {
        OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        double cpuLoad = osBean.getSystemCpuLoad() * 100;
        log.info("current host cpu usage:{}", cpuLoad);
        return cpuLoad > WARNING_THRESHOLD;
    }

    /**
     * 计算 Java 虚拟机（JVM）的 CPU 负载情况。
     * 使用 OperatingSystemMXBean 获取进程 CPU 负载，并计算 JVM CPU 使用率百分比。
     *
     * @return 如果 JVM CPU 使用率大于 WARNING_THRESHOLD%，则返回 true，否则返回 false。
     */
    public static boolean computeJvmCpuOverLoad() {
        OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
        double cpuLoad = osBean.getProcessCpuLoad() * 100;
        log.info("current java cpu usage:{}", cpuLoad);
        return cpuLoad > WARNING_THRESHOLD;
    }

    /**
     * 计算主机的内存负载情况。
     * 使用 OperatingSystemMXBean 获取主机的总物理内存大小和可用物理内存大小，
     * 然后计算主机内存使用百分比。
     *
     * @return 如果主机内存使用率大于 WARNING_THRESHOLD%，则返回 true，否则返回 false。
     */
    public static boolean computeHostMemoryOverLoad() {
        // 获取操作系统MXBean实例
        OperatingSystemMXBean osBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        // 获取系统内存信息
        long totalPhysicalMemorySize = osBean.getTotalPhysicalMemorySize();
        long freePhysicalMemorySize = osBean.getFreePhysicalMemorySize();
        long usedPhysicalMemorySize = totalPhysicalMemorySize - freePhysicalMemorySize;
        // 计算内存使用百分比
        double memoryUsagePercentage = (usedPhysicalMemorySize * 100.0) / totalPhysicalMemorySize;
        log.info("current host memory usage:{}", memoryUsagePercentage);
        return memoryUsagePercentage > WARNING_THRESHOLD;
    }

    /**
     * 计算 Java 虚拟机（JVM）的内存负载情况。
     * 使用 Runtime 类获取当前 JVM 的总内存和可用内存，
     * 然后计算 JVM 内存使用百分比。
     *
     * @return 如果 JVM 内存使用率大于 WARNING_THRESHOLD%，则返回 true，否则返回 false。
     */
    public static boolean computeJvmMemoryOverLoad() {
        // 获取当前JVM的内存信息
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        // 计算内存使用百分比
        double memoryUsagePercentage = (usedMemory * 100.0) / totalMemory;
        // 计算内存使用率
        log.info("current java stack usage:{}", memoryUsagePercentage);
        return memoryUsagePercentage > WARNING_THRESHOLD;
    }
}
