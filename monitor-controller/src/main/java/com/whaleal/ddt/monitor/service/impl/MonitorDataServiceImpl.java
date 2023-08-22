package com.whaleal.ddt.monitor.service.impl;


import com.alibaba.fastjson.JSON;
import com.whaleal.ddt.monitor.service.MonitorDataService;
import org.springframework.stereotype.Service;

import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * @author liheping
 */
@Service
public class MonitorDataServiceImpl implements MonitorDataService {

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
}
