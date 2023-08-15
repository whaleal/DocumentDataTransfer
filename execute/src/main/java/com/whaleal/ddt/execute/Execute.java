//package com.whaleal.ddt.execute;
//
//import com.whaleal.ddt.execute.config.Property;
//import com.whaleal.ddt.execute.config.WorkInfo;
//import com.whaleal.ddt.execute.config.WorkInfoGenerator;
//import com.whaleal.ddt.task.CommonTask;
//import com.whaleal.ddt.util.HostInfoUtil;
//import lombok.extern.log4j.Log4j2;
//
///**
// * @projectName: DocumentDataTransfer
// * @package: com.whaleal.ddt.execute
// * @className: Execute
// * @author: Eric
// * @description: TODO
// * @date: 14/08/2023 17:42
// * @version: 1.0
// */
//@Log4j2
//public class Execute {
//    static {
//        log.info("D2T启动信息:hostName[{}],pid[{}],启动目录:[{}]", HostInfoUtil.getHostName(), HostInfoUtil.getProcessID(), HostInfoUtil.getProcessDir());
//        log.info("JVM Info:{}", HostInfoUtil.getJvmArg());
//        log.info("\n" +
//                "  ____    ____    _____ \n" +
//                " |  _ \\  |___ \\  |_   _|\n" +
//                " | | | |   __) |   | |  \n" +
//                " | |_| |  / __/    | |  \n" +
//                " |____/  |_____|   |_|  \n" +
//                "                        ");
//        log.info("\nDocument Data Transfer - An open-source project licensed under GPL+SSPL\n" +
//                "   \n" +
//                "Copyright (C) [2023 - present ] [Whaleal]\n" +
//                "   \n" +
//                "This program is free software; you can redistribute it and/or modify it under the terms\n" +
//                "of the GNU General Public License and Server Side Public License (SSPL) as published by\n" +
//                "the Free Software Foundation; either version 2 of the License, or (at your option) any later version.\n" +
//                "    \n" +
//                "This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;\n" +
//                "without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.\n" +
//                "See the GNU General Public License and SSPL for more details.\n" +
//                "  \n" +
//                "For more information, visit the official website: [www.whaleal.com]");
//        CommonTask.copyRight();
//    }
//
//    public static void main(String[] args) {
//        // 设置配置文件的路径
//        Property.setFileName("/Users/liheping/Desktop/project/DocumentDataTransfer/execute/src/main/resources/DDT.properties");
//        // 检查是否传入了正确的启动参数
//        if (args.length == 1) {
//            // 如果只传入一个参数，则将该参数作为配置文件的路径
//            if (args[0] == null || args[0].length() == 0) {
//                // 输出错误信息
//                log.error("enter the correct path to the configuration file");
//                return;
//            }
//            Property.setFileName(args[0]);
//        } else if (args.length == 2) {
//            // 如果传入了两个参数，根据实际情况进行处理
//            // (根据代码这里是空的，可能还有其他处理逻辑)
//        } else {
//            // 参数数量错误，输出错误信息
//            log.error("start parameter error");
//        }
//        // 生成工作信息
//        final WorkInfo workInfo = WorkInfoGenerator.generateWorkInfo();
//        // 启动任务
//        startRealTimeChangeStream(workInfo);
//        // 退出程序
//        System.exit(0);
//    }
//
//
//    private static void startFullSync(final WorkInfo workInfo) {
//        String workName = workInfo.getWorkName();
//        workInfo.setWorkName(workName + "_full");
//        FullSync.startFullSync(workInfo);
//    }
//
//    private static void startRealTimeOplog(final WorkInfo workInfo) {
//        String workName = workInfo.getWorkName();
//        workInfo.setWorkName(workName + "_realTime");
//        RealTimeOplog.startRealTimeOplog(workInfo);
//    }
//
//    private static void startRealTimeChangeStream(final WorkInfo workInfo) {
//        String workName = workInfo.getWorkName();
//        workInfo.setWorkName(workName + "_realTime");
//        RealTimeChangeStream.startRealTimeChangeStream(workInfo);
//    }
//
//}
