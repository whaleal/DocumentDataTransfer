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
