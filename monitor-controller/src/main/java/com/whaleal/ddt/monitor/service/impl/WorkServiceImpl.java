package com.whaleal.ddt.monitor.service.impl;


import com.whaleal.ddt.monitor.service.WorkService;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author liheping
 */
@Service
public class WorkServiceImpl implements WorkService {


    /**
     * workInfo 信息
     * k:workName,v:workInfoEntity
     * 数据量不大 可以放内存
     */
    private static final Map<String, Map<Object, Object>> WORK_INFO_MAP = new ConcurrentHashMap<>();

    @Override
    public void upsertWorkInfo(String workName, Map<Object, Object> workInfo) {
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






}
