package com.whaleal.ddt.monitor.service;

import java.util.List;
import java.util.Map;

/**
 * @projectName: full-common
 * @package: com.whaleal.ddt.monitor.service.impl
 * @className: MonitorDataServiceImpl
 * @author: Eric
 * @description: TODO
 * @date: 22/08/2023 11:24
 * @version: 1.0
 */
public interface MonitorDataService {
    void saveHostData(Map<Object, Object> map);

    void saveFullWorkData(String workName, Map<Object, Object> map);

    void saveRealTimeWorkData(String workName, Map<Object, Object> map);

    Map<String, List<Object>> getHostMonitor( List<String> typeList, long startTime, long endTime);

    Map<String, List<Object>> getFullWorkMonitor(String workName, List<String> typeList, long startTime, long endTime);

    Map<String, List<Object>> getRealTimeWorkMonitor(String workName, List<String> typeList, long startTime, long endTime);

    Map<String, Object> getWorkMonitor(String workName,
                                       long startTime,
                                       long endTime,
                                       String type);
}
