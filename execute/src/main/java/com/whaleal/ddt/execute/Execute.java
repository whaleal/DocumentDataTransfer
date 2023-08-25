package com.whaleal.ddt.execute;

import com.alibaba.fastjson2.JSON;
import com.whaleal.ddt.execute.config.Property;
import com.whaleal.ddt.execute.config.WorkInfo;
import com.whaleal.ddt.execute.config.WorkInfoGenerator;
import com.whaleal.ddt.execute.full.common.BaseFullWork;
import com.whaleal.ddt.execute.realtime.common.BaseRealTimeWork;
import com.whaleal.ddt.task.CommonTask;
import com.whaleal.ddt.util.HostInfoUtil;
import lombok.extern.log4j.Log4j2;

import java.util.Map;
import java.util.TreeMap;


/**
 * @author liheping
 */
@Log4j2
public class Execute {


    static {
        Map<String, Object> jvmInfo = new TreeMap<>();
        jvmInfo.put("JVMArg", HostInfoUtil.getJvmArg());
        jvmInfo.put("hostName", HostInfoUtil.getHostName());
        jvmInfo.put("pid", HostInfoUtil.getProcessID());
        jvmInfo.put("bootDirectory", HostInfoUtil.getProcessDir());
        log.info("D2T Boot information :{}", JSON.toJSONString(jvmInfo));

        log.info("\n" +
                "  ____    ____    _____ \n" +
                " |  _ \\  |___ \\  |_   _|\n" +
                " | | | |   __) |   | |  \n" +
                " | |_| |  / __/    | |  \n" +
                " |____/  |_____|   |_|  \n" +
                "                        ");
        log.info("\nDocument Data Transfer - An open-source project licensed under GPL+SSPL\n" +
                "   \n" +
                "Copyright (C) [2023 - present ] [Whaleal]\n" +
                "   \n" +
                "This program is free software; you can redistribute it and/or modify it under the terms\n" +
                "of the GNU General Public License and Server Side Public License (SSPL) as published by\n" +
                "the Free Software Foundation; either version 2 of the License, or (at your option) any later version.\n" +
                "    \n" +
                "This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;\n" +
                "without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.\n" +
                "See the GNU General Public License and SSPL for more details.\n" +
                "  \n" +
                "For more information, visit the official website: [www.whaleal.com]");
        CommonTask.copyRight();
    }

    public static void main(String[] args) {
        // 设置配置文件的路径
        Property.setFileName("/Users/liheping/Desktop/project/DocumentDataTransfer/execute/src/main/resources/DDT.properties");
        // 检查是否传入了正确的启动参数
        if (args.length == 1) {
            // 如果只传入一个参数，则将该参数作为配置文件的路径
            if (args[0] == null || args[0].length() == 0) {
                // 输出错误信息
                log.error("enter the correct path to the configuration file");
                return;
            }
            Property.setFileName(args[0]);
        } else if (args.length == 2) {
            // 如果传入了两个参数，根据实际情况进行处理
            // (根据代码这里是空的，可能还有其他处理逻辑)
        } else {
            // 参数数量错误，输出错误信息
            log.error("start parameter error");
        }
        // 生成工作信息
        WorkInfo workInfo = WorkInfoGenerator.generateWorkInfo();
        // 启动任务
        start(workInfo, workInfo.getFullType(), workInfo.getRealTimeType());
        // 退出程序
        System.exit(1);
    }

    /**
     * 启动任务的方法
     *
     * @param workInfo 工作信息
     */
    private static void start(final WorkInfo workInfo, final String fullType, final String realTimeType) {
        // 获取工作名称
        final String workName = workInfo.getWorkName();
        final String syncMode = workInfo.getSyncMode();
        // 根据同步模式选择不同的启动方式
        if (syncMode.equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL)) {
            workInfo.setStartTime(System.currentTimeMillis());
            workInfo.setWorkName(workName + "_full");
            // 全量同步模式
            workInfo.setStartTime(System.currentTimeMillis());
            startFull(workInfo, fullType);

        } else if (syncMode.equalsIgnoreCase(WorkInfo.SYNC_MODE_REAL_TIME)) {
            workInfo.setWorkName(workName + "_realTime");
            // 实时同步模式
            startRealTime(workInfo, realTimeType);

        } else if (syncMode.equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_INCREMENT)) {
            // 全量+增量同步模式
            // 先执行全量同步，然后再执行增量同步
            workInfo.setStartOplogTime((int) (System.currentTimeMillis() / 1000));
            workInfo.setWorkName(workName + "_full");
            workInfo.setSyncMode(WorkInfo.SYNC_MODE_ALL);
            startFull(workInfo, fullType);

            // 设置新的任务的时区
            // Q: 增量任务 也可以加上进度百分比
            // A: 已在ReadOplog 增加进度百分比
            workInfo.setEndOplogTime((int) (System.currentTimeMillis() / 1000));
            workInfo.setStartTime(System.currentTimeMillis());
            workInfo.setWorkName(workName + "_realTime");
            workInfo.setSyncMode(WorkInfo.SYNC_MODE_REAL_TIME);
            startRealTime(workInfo, realTimeType);

        } else if (syncMode.equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_REAL_TIME)) {
            // 全量+实时同步模式
            // 先执行全量同步，然后再执行实时同步
            workInfo.setStartOplogTime((int) (System.currentTimeMillis() / 1000));
            workInfo.setWorkName(workName + "_full");
            workInfo.setSyncMode(WorkInfo.SYNC_MODE_ALL);
            startFull(workInfo, fullType);
            workInfo.setEndOplogTime(0);

            workInfo.setStartTime(System.currentTimeMillis());
            // 设置新的任务的时区
            workInfo.setWorkName(workName + "_realTime");
            workInfo.setSyncMode(WorkInfo.SYNC_MODE_REAL_TIME);
            startRealTime(workInfo, realTimeType);

        }
    }


    private static void startFull(final WorkInfo workInfo, final String fullType) {
        BaseFullWork.startFull(workInfo, fullType);

    }

    private static void startRealTime(final WorkInfo workInfo, final String realTimeType) {
        BaseRealTimeWork.startRealTime(workInfo, realTimeType);
    }
}
