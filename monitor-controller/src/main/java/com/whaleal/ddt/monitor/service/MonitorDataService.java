package com.whaleal.ddt.monitor.service;

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
}
