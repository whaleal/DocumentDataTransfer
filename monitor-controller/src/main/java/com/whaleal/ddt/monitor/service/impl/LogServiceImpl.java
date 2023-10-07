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
