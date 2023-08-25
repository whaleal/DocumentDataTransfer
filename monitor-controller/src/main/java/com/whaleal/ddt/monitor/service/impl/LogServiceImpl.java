package com.whaleal.ddt.monitor.service.impl;

import com.alibaba.fastjson.JSON;
import com.whaleal.ddt.monitor.model.LogEntity;
import com.whaleal.ddt.monitor.service.LogService;
import com.whaleal.icefrog.core.util.StrUtil;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


/**
 * @author liheping
 */
@Service
public class LogServiceImpl implements LogService {


    private static String monitorDataDir = "../monitorDataDir/";

    @Override
    public List<LogEntity> findLog(String type, long startTime, long endTime, String info, Integer pageIndex, Integer pageSize) {
        List<LogEntity> logEntityList = new ArrayList<>();
        long totalAddNum = 0;
        try (BufferedReader br = new BufferedReader(new FileReader(monitorDataDir + "/DDT.log"))) {
            String line;
            while ((line = br.readLine()) != null) {

                LogEntity logEntity = JSON.parseObject(line, LogEntity.class);
                if (logEntity.getTime() < startTime || logEntity.getTime() > endTime) {
                    continue;
                }
                if (StrUtil.isNotBlank(type) && !logEntity.getType().equalsIgnoreCase(type)) {
                    continue;
                }
                if (StrUtil.isNotBlank(info) && !logEntity.getInfo().contains(info)) {
                    continue;
                }

                logEntityList.add(logEntity);
                totalAddNum++;
                if (logEntityList.size() > pageSize) {
                    logEntityList.remove(0);
                }
                if (totalAddNum > ((long) (pageIndex + 1) * pageIndex+pageSize)) {
                    break;
                }
            }
        } catch (Exception e) {
        }
        // 直接从具体源日志读取
        return logEntityList;
    }


    @Override
    public void saveLog(LogEntity logEntity) {
        try (FileOutputStream fileOutputStream = new FileOutputStream(monitorDataDir + "/DDT.log", true)) {
            fileOutputStream.write((JSON.toJSON(logEntity) + "\r\n").getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
        }
    }
}
