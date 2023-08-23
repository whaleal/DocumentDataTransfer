package com.whaleal.ddt.monitor.service;

import java.util.List;
import java.util.Map;

/**
 * MonitorDataService 接口
 *
 * This service interface defines methods related to monitoring data.
 *
 * Project: full-common
 * Package: com.whaleal.ddt.monitor.service.impl
 * Class: MonitorDataService
 * Author: Eric
 * Description: This interface provides methods for saving and retrieving monitoring data.
 * Date: 22/08/2023 11:24
 * Version: 1.0
 */
public interface MonitorDataService {

    /**
     * 保存主机数据
     *
     * Saves host monitoring data.
     *
     * @param map  数据集合
     */
    void saveHostData(Map<Object, Object> map);

    /**
     * 保存完整工作数据
     *
     * Saves complete work monitoring data.
     *
     * @param workName  工作名称
     * @param map  数据集合
     */
    void saveFullWorkData(String workName, Map<Object, Object> map);

    /**
     * 保存实时工作数据
     *
     * Saves real-time work monitoring data.
     *
     * @param workName  工作名称
     * @param map  数据集合
     */
    void saveRealTimeWorkData(String workName, Map<Object, Object> map);

    /**
     * 获取主机监控数据
     *
     * Retrieves host monitoring data.
     *
     * @param typeList  类型列表
     * @param startTime  起始时间
     * @param endTime  结束时间
     * @return 主机监控数据集合
     */
    Map<String, List<Object>> getHostMonitor(List<String> typeList, long startTime, long endTime);

    /**
     * 获取完整工作监控数据
     *
     * Retrieves complete work monitoring data.
     *
     * @param workName  工作名称
     * @param typeList  类型列表
     * @param startTime  起始时间
     * @param endTime  结束时间
     * @return 完整工作监控数据集合
     */
    Map<String, List<Object>> getFullWorkMonitor(String workName, List<String> typeList, long startTime, long endTime);

    /**
     * 获取实时工作监控数据
     *
     * Retrieves real-time work monitoring data.
     *
     * @param workName  工作名称
     * @param typeList  类型列表
     * @param startTime  起始时间
     * @param endTime  结束时间
     * @return 实时工作监控数据集合
     */
    Map<String, List<Object>> getRealTimeWorkMonitor(String workName, List<String> typeList, long startTime, long endTime);

    /**
     * 获取工作监控数据
     *
     * Retrieves work monitoring data.
     *
     * @param workName  工作名称
     * @param startTime  起始时间
     * @param endTime  结束时间
     * @param type  监控数据类型
     * @return 工作监控数据
     */
    Map<String, Object> getWorkMonitor(String workName, long startTime, long endTime, String type);
}
