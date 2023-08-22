package com.whaleal.ddt.monitor.service.impl;

import com.whaleal.ddt.monitor.model.LogEntity;
import com.whaleal.ddt.monitor.service.LogService;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;


/**
 * @author liheping
 */
@Service
public class LogServiceImpl implements LogService {


    @Override
    public List<LogEntity> findLog(String processId, String type, long startTime, long endTime, String info, Integer pageIndex, Integer pageSize) {
        // 直接从具体源日志读取
        return new ArrayList<>();
    }

    @Override
    public long findLogCount(String processId, String type, long startTime, long endTime, String info) {
        return 0;
    }


}
