///*
// * Document Data Transfer - An open-source project licensed under GPL+SSPL
// *
// * Copyright (C) [2023 - present ] [Whaleal]
// *
// * This program is free software; you can redistribute it and/or modify it under the terms
// * of the GNU General Public License and Server Side Public License (SSPL) as published by
// * the Free Software Foundation; either version 2 of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
// * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
// * See the GNU General Public License and SSPL for more details.
// *
// * For more information, visit the official website: [www.whaleal.com]
// */
//package com.whaleal.ddt.execute;
//
//
//import com.whaleal.ddt.execute.config.WorkInfo;
//import com.whaleal.ddt.status.WorkStatus;
//import com.whaleal.ddt.sync.cache.MetadataOplog;
//import lombok.extern.log4j.Log4j2;
//
//import java.util.concurrent.TimeUnit;
//
///**
// * @author liheping
// */
//@Log4j2
//public class Execute_FullSync_RealTimeChangeStream {
//
//
//
//
//
//
//
//
//    /**
//     * 启动任务的方法
//     *
//     * @param workInfo 工作信息
//     */
//    public void start(final WorkInfo workInfo) {
////        // 获取工作名称
////        String workName = workInfo.getWorkName();
////        // 根据同步模式选择不同的启动方式
////        if (workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL)) {
////            workInfo.setWorkName(workName + "_full");
////            // 全量同步模式
////
////            startFullSync(workInfo);
////        } else if (workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_REAL_TIME)) {
////            workInfo.setWorkName(workName + "_realTime");
////            // 实时同步模式
////            startRealTimeChangeStream(workInfo);
////        } else if (workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_INCREMENT)) {
////            // 全量+增量同步模式
////            // 先执行全量同步，然后再执行增量同步
////            workInfo.setStartOplogTime((int) (System.currentTimeMillis() / 1000));
////            workInfo.setWorkName(workName + "_full");
////            startFullSync(workInfo);
////
////            // 设置新的任务的时区
////            // Q: 增量任务 也可以加上进度百分比
////            // A: 已在ReadOplog 增加进度百分比
////            workInfo.setEndOplogTime((int) (System.currentTimeMillis() / 1000));
////            workInfo.setStartTime(System.currentTimeMillis());
////            workInfo.setWorkName(workName + "_realTime");
////            startRealTimeChangeStream(workInfo);
////        } else if (workInfo.getSyncMode().equalsIgnoreCase(WorkInfo.SYNC_MODE_ALL_AND_REAL_TIME)) {
////            // 全量+实时同步模式
////            // 先执行全量同步，然后再执行实时同步
////            workInfo.setStartOplogTime((int) (System.currentTimeMillis() / 1000));
////            workInfo.setWorkName(workName + "_full");
////            startFullSync(workInfo);
////            workInfo.setStartTime(System.currentTimeMillis());
////            // 设置新的任务的时区
////            workInfo.setWorkName(workName + "_realTime");
////            startRealTimeChangeStream(workInfo);
//        }
//    }
//
//
//
//
//
//}
