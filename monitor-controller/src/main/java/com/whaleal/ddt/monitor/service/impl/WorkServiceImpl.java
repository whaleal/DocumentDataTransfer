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
 * @author liheping
 */
@Service
@Log4j2
public class WorkServiceImpl implements WorkService {


    /**
     * workInfo 信息
     * k:workName,v:workInfoEntity
     * 数据量不大 可以放内存
     */
    private static final Map<String, Map<Object, Object>> WORK_INFO_MAP = new ConcurrentHashMap<>();

    @Override
    public void upsertWorkInfo(String workName, Map<Object, Object> workInfo) {
        log.info("upsertWorkInfo:{}", JSON.toJSONString(workInfo));
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
        return result;
    }





}
