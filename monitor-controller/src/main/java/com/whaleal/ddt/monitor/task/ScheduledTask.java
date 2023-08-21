//package com.whaleal.ddt.monitor.task;
//
//
//import com.alibaba.fastjson.JSONObject;
//import com.whaleal.ddt.monitor.service.WorkService;
//import com.whaleal.mongot.model.ConfigEntity;
//import com.whaleal.mongot.service.ConfigService;
//import com.whaleal.mongot.service.WorkService;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.scheduling.annotation.EnableScheduling;
//import org.springframework.scheduling.annotation.Scheduled;
//
//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.util.List;
//import java.util.Map;
//
///**
// * @author liheping
// */
//@Configuration
//@EnableScheduling
//public class ScheduledTask {
//
//    @Autowired
//    private WorkService taskService;
//    @Autowired
//    private ConfigService configService;
//
//    @Scheduled(cron = "0 0/1 * * * ? ")
//    private void checkState() {
//        taskService.setTaskInfoStatus();
//    }
//
//
//    @Scheduled(cron = "0 0/1 * * * ? ")
//    private void saveTaskInfo() {
//        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("taskInfo.txt", false))) {
//            List<Map<String, Object>> taskInfo = taskService.getAllTaskInfo("", "", 0, Integer.MAX_VALUE);
//            for (Map<String, Object> map : taskInfo) {
//                bufferedWriter.write(JSONObject.toJSONString(map) + "\n");
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Scheduled(cron = "0 0/1 * * * ? ")
//    private void saveConfig() {
//        try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter("config.txt", false))) {
//            List<ConfigEntity> configList = configService.findConfig("", "", 0, Integer.MAX_VALUE);
//            for (ConfigEntity configEntity : configList) {
//                bufferedWriter.write(JSONObject.toJSONString(configEntity) + "\n");
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//}
