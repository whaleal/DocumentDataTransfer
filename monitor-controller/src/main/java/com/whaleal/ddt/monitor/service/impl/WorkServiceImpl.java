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
import com.whaleal.ddt.monitor.service.WorkService;
import com.whaleal.icefrog.core.util.StrUtil;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * WorkServiceImpl 类
 * <p>
 * This class provides an implementation of the WorkService interface for managing work information.
 * <p>
 * Author: liheping
 */
@Service
@Log4j2
public class WorkServiceImpl implements WorkService {

    /**
     * 存储工作信息
     * Key: workName, Value: workInfoEntity
     * Since the data size is not large, it can be stored in memory.
     */
    private static final Map<String, Map<Object, Object>> WORK_INFO_MAP = new ConcurrentHashMap<>();

    @Override
    public void upsertWorkInfo(String workName, Map<Object, Object> workInfo) {
        log.info("upsertWorkInfo: {}", JSON.toJSONString(workInfo));
        for (Map.Entry<String, Map<Object, Object>> entry : WORK_INFO_MAP.entrySet()) {
            if ((!entry.getValue().containsKey("endTime"))
                    || "0".equals(entry.getValue().get("endTime").toString()) ||
                    entry.getValue().get("endTime").toString().equals(Long.MAX_VALUE + "")) {
                entry.getValue().put("endTime", workInfo.get("startTime"));
            }
        }

        if (WORK_INFO_MAP.containsKey(workName)) {
            WORK_INFO_MAP.get(workName).putAll(workInfo);
        } else {
            WORK_INFO_MAP.put(workName, workInfo);
        }
    }

    @Override
    public Map<Object, Object> getWorkInfo(String workName) {
        return WORK_INFO_MAP.get(workName);
    }

    @Override
    public List<Map<Object, Object>> getWorkInfoList(String workName) {
        List<Map<Object, Object>> result = new ArrayList<>();
        for (Map.Entry<String, Map<Object, Object>> entry : WORK_INFO_MAP.entrySet()) {
            if (StrUtil.isBlank(workName) || entry.getKey().contains(workName)) {
                result.add(entry.getValue());
            }
        }
        result.sort((o1, o2) -> o2.get("startTime").toString().compareTo(o1.get("startTime").toString()));
        return result;
    }
}
