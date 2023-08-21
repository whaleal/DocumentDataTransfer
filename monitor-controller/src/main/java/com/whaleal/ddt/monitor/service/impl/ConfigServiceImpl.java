//package com.whaleal.ddt.monitor.service.impl;
//
//import com.alibaba.fastjson.JSONObject;
//import com.whaleal.icefrog.core.util.StrUtil;
//import com.whaleal.mongot.model.ConfigEntity;
//import com.whaleal.mongot.service.ConfigService;
//import org.springframework.stereotype.Service;
//
//import java.io.BufferedReader;
//import java.io.File;
//import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//
///**
// * @author cc
// */
//@Service
//public class ConfigServiceImpl implements ConfigService {
//
//    private static final Map<String, ConfigEntity> CONFIG_ENTITY_MAP = new ConcurrentHashMap<>();
//
//    static {
//        File taskInfoFile = new File("config.txt");
//        if (!taskInfoFile.exists()) {
//            try {
//                taskInfoFile.createNewFile();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//        try (BufferedReader br = Files.newBufferedReader(Paths.get("config.txt"))) {
//            String line;
//            while ((line = br.readLine()) != null) {
//                ConfigEntity configEntity = JSONObject.parseObject(line, ConfigEntity.class);
//                CONFIG_ENTITY_MAP.put(configEntity.getTaskName(), configEntity);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Override
//    public ConfigEntity saveConfig(ConfigEntity configEntity) {
//
//        if (configEntity.getSourceThreadNum() != null && (configEntity.getSourceThreadNum() < 2 || configEntity.getSourceThreadNum() > 50)) {
//            configEntity.setSourceThreadNum(2);
//        }
//        if (configEntity.getTargetThreadNum() != null && (configEntity.getTargetThreadNum() < 4 || configEntity.getTargetThreadNum() > 50)) {
//            configEntity.setTargetThreadNum(6);
//        }
//        if (configEntity.getCacheBucketSize() != null && (configEntity.getCacheBucketSize() < 10 || configEntity.getCacheBucketSize() > 100)) {
//            configEntity.setCacheBucketSize(20);
//        }
//        if (configEntity.getCacheBucketNum() != null && (configEntity.getCacheBucketNum() < 10 || configEntity.getCacheBucketNum() > 100)) {
//            configEntity.setCacheBucketNum(20);
//        }
//        if (configEntity.getDataBatchSize() != null && (configEntity.getDataBatchSize() < 8 || configEntity.getDataBatchSize() > 1024)) {
//            configEntity.setDataBatchSize(128);
//        }
//        if (configEntity.getRealTimeThreadNum() != null && (configEntity.getRealTimeThreadNum() < 4 || configEntity.getRealTimeThreadNum() > 50)) {
//            configEntity.setRealTimeThreadNum(8L);
//        }
//        if (configEntity.getDelayTime() != null && (configEntity.getDelayTime() != null && configEntity.getDelayTime() < 0)) {
//            configEntity.setDelayTime(1L);
//        }
//        if (configEntity.getSyncMode() != null && ("all".equals(configEntity.getSyncMode()))) {
//            configEntity.setStartOplogTime(null);
//            configEntity.setEndOplogTime(null);
//            configEntity.setDelayTime(null);
//            configEntity.setRealTimeThreadNum(null);
//        }
//        if (configEntity.getSyncMode() != null && ("allAndIncrement".equals(configEntity.getSyncMode()))) {
//            configEntity.setEndOplogTime(null);
//            configEntity.setDelayTime(null);
//        }
//
//        if (configEntity.getSyncMode() != null && ("realTime".equals(configEntity.getSyncMode()))) {
//            configEntity.setParallelSynchronizationMultipleTables(null);
//            configEntity.setAutoDropExistDbTable(null);
//            configEntity.setAutoCreateIndex(null);
//            configEntity.setSourceThreadNum(null);
//            configEntity.setTargetThreadNum(null);
//            configEntity.setClusterDDL("");
//        }
//
//        configEntity.setId(configEntity.getTaskName());
//        CONFIG_ENTITY_MAP.put(configEntity.getId(), configEntity);
//        return configEntity;
//    }
//
//    @Override
//    public Boolean checkTaskName(String taskName) {
//        for (Map.Entry<String, ConfigEntity> configEntry : CONFIG_ENTITY_MAP.entrySet()) {
//            if (configEntry.getValue().getTaskName().equals(taskName)) {
//                return false;
//            }
//        }
//        return true;
//    }
//
//
//    @Override
//    public void deleteConfig(String taskName) {
//        CONFIG_ENTITY_MAP.entrySet().removeIf(configEntry -> configEntry.getValue().getTaskName().equals(taskName));
//    }
//
//    @Override
//    public List<ConfigEntity> findConfig(String name, String url, Integer pageIndex, Integer pageSize) {
//        long skip = (long) (pageIndex - 1) * pageSize;
//        List<ConfigEntity> configList = new ArrayList<>();
//        for (Map.Entry<String, ConfigEntity> configEntry : CONFIG_ENTITY_MAP.entrySet()) {
//            ConfigEntity config = configEntry.getValue();
//            if (StrUtil.isNotBlank(name)) {
//                if (!config.getTaskName().contains(name)) {
//                    continue;
//                }
//            }
//            if (StrUtil.isNotBlank(url)) {
//                if ((!config.getSourceDsUrl().contains(url)) && (!config.getTargetDsUrl().contains(url))) {
//                    continue;
//                }
//            }
//            if (skip-- <= 0 && configList.size() < pageSize) {
//                configList.add(config);
//            }
//            if (configList.size() >= pageSize) {
//                break;
//            }
//        }
//        Collections.sort(configList, new Comparator<ConfigEntity>() {
//            @Override
//            public int compare(ConfigEntity o1, ConfigEntity o2) {
//                return o1.getProName().compareTo(o2.getProName());
//            }
//        });
//        return configList;
//    }
//
//    @Override
//    public long findConfigCount(String name, String url) {
//        long count = 0;
//        for (Map.Entry<String, ConfigEntity> configEntry : CONFIG_ENTITY_MAP.entrySet()) {
//            ConfigEntity config = configEntry.getValue();
//            if (StrUtil.isNotBlank(name)) {
//                if (!config.getTaskName().contains(name)) {
//                    continue;
//                }
//            }
//            if (StrUtil.isNotBlank(url)) {
//                if ((!config.getSourceDsUrl().contains(url)) && (!config.getTargetDsUrl().contains(url))) {
//                    continue;
//                }
//            }
//            count++;
//        }
//        return count;
//    }
//
//
//    @Override
//    public ConfigEntity findConfigByTaskName(String taskName) {
//        for (Map.Entry<String, ConfigEntity> entry : CONFIG_ENTITY_MAP.entrySet()) {
//            if (entry.getValue().getTaskName().equals(taskName)) {
//                return entry.getValue();
//            }
//        }
//        return null;
//    }
//
//}
